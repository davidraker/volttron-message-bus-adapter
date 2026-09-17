# -*- coding: utf-8 -*- {{{
# ===----------------------------------------------------------------------===
#
#                 Installable Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2022 Battelle Memorial Institute
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# ===----------------------------------------------------------------------===
# }}}

from pydantic import BaseModel,ConfigDict, Field, model_validator


# TODO: What fields do these all need?
# TODO: How do we populate them from the config dict?
class MessageBusConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True)


# TODO: MQTT and NATS configurations should be in their respective repos, but how do they get used/imported here?
class MQTTConfig(MessageBusConfig):
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True)
    host: str
    port: int = 1883
    keepalive: int = 60
    bind_address: str = ''
    bind_port: int = 0


class NATSConfig(MessageBusConfig):
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True)


class MessageBusAdapterConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True)
    bus_type: str
    adapters: list[MessageBusConfig] = Field(default_factory=list[MessageBusConfig],
                                             description="List of bus adapter configurations.")

    @model_validator(mode='before')
    def validate_adapters(cls, data: dict):
        match data['bus_type']:
            case 'mqtt':
                adapter_config = MQTTConfig
            case 'nats':
                adapter_config = NATSConfig
            case _:
                adapter_config = MessageBusConfig

        data.update({'adapters': [adapter_config(**a) for a in data['adapters']]})
        return data
