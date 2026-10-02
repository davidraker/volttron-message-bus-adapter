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

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SerializeAsAny, field_validator, model_validator


class MessageBusConfig(BaseModel):
    """Configuration for a single remote message bus connection.

    Every field other than ``name`` is forwarded to the protocol proxy process as a
    command line argument (``--field-name value``), so field names must match the
    proxy class's constructor parameters. ``extra='allow'`` lets bus types without a
    dedicated config class pass arbitrary parameters through to their proxy.
    """
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True, extra='allow')

    name: str | None = Field(default=None,
                             description='Optional stable handle for this remote. When set, the'
                                         ' unique_remote_id is (bus_type, name).')
    local_subscriptions: list[str] = Field(
        default_factory=list, exclude=True,
        description='Remote topics to serve from local data as soon as the proxy is up: each is resolved through'
                    ' platform.presentation to a canonical resource whose publications are transformed and'
                    ' published to that remote topic continuously (the same as a SUBSCRIBE_LOCAL request).')
    remote_subscriptions: list[str] = Field(
        default_factory=list, exclude=True,
        description='Remote topics the proxy subscribes to on the foreign bus as soon as it is up; messages'
                    ' arriving on them are relayed into VOLTTRON (the same as the subscribe RPC).')

    def unique_remote_id(self, bus_type: str) -> tuple:
        """Identifier used by callers and the ProtocolProxyManager to address this remote."""
        return (bus_type, self.name) if self.name else (bus_type, *self._identity())

    def _identity(self) -> tuple:
        """Fallback identity when no name is given: the hashable proxy parameters, in order."""
        return tuple(v for v in self.proxy_kwargs().values() if isinstance(v, (str, int, float, bool)))

    def proxy_kwargs(self) -> dict[str, Any]:
        """Parameters passed to ProtocolProxyManager.get_proxy() and on to the proxy process. The subscription
        lists are the adapter's own (``exclude=True``) and never reach the proxy command line."""
        return self.model_dump(exclude={'name'}, exclude_none=True)


class MQTTConfig(MessageBusConfig):
    """Parameters of protocol_proxy.protocol.mqtt.MQTTProxy. Unset optional values are not sent."""
    host: str
    port: int = 1883
    keepalive: int = 60
    bind_address: str = ''
    bind_port: int = 0
    client_id: str | None = None
    username: str | None = None
    password: str | None = None
    tls: bool | None = None
    protocol: Literal['MQTTv31', 'MQTTv311', 'MQTTv5'] | None = None
    qos: Literal[0, 1, 2] | None = None
    reconnect_min_delay: float | None = Field(default=None, gt=0)
    reconnect_max_delay: float | None = Field(default=None, gt=0)

    def _identity(self) -> tuple:
        return self.host, self.port


class NATSConfig(MessageBusConfig):
    """Parameters of protocol_proxy.protocol.nats.NATSProxy.

    ``servers`` is one URL or a list of URLs; it is sent to the proxy as a comma-separated string.
    """
    servers: str | list[str]
    name: str | None = None
    user: str | None = None
    password: str | None = None
    nats_token: str | None = None
    connect_timeout: float | None = Field(default=None, gt=0)
    max_reconnect_attempts: int | None = None
    reconnect_time_wait: float | None = Field(default=None, gt=0)
    tls: bool | None = None

    @field_validator('servers')
    @classmethod
    def _join_servers(cls, value: str | list[str]) -> str:
        urls = value.split(',') if isinstance(value, str) else value
        urls = [u.strip() for u in urls if u and u.strip()]
        if not urls:
            raise ValueError('At least one NATS server URL is required.')
        return ','.join(urls)

    def _identity(self) -> tuple:
        return (self.servers,)


# TODO: Bus-specific config classes belong with their proxy plugins. Until the plugins
#  expose them (e.g. a CONFIG_CLASS attribute next to PROXY_CLASS), they live here.
ADAPTER_CONFIG_CLASSES: dict[str, type[MessageBusConfig]] = {
    'mqtt': MQTTConfig,
    'nats': NATSConfig,
}


class MessageBusAdapterConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True, populate_by_name=True)

    bus_type: str = Field(default='', description='Protocol plugin name, e.g. "mqtt" or "nats".'
                                                  ' Resolved to protocol_proxy.protocol.<bus_type>.')
    adapters: list[SerializeAsAny[MessageBusConfig]] = Field(
        default_factory=list, description='List of remote bus connections to proxy.')
    proxy_registration_timeout: float = Field(
        default=30.0, gt=0, description='Seconds to wait for a newly launched proxy to register.')

    @model_validator(mode='before')
    @classmethod
    def _build_adapters(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        data = dict(data)
        bus_type = str(data.get('bus_type') or '').strip().lower()
        data['bus_type'] = bus_type
        config_class = ADAPTER_CONFIG_CLASSES.get(bus_type, MessageBusConfig)
        adapters = data.get('adapters') or []
        if not isinstance(adapters, (list, tuple)):
            raise ValueError(f'"adapters" must be a list, got {type(adapters).__name__}.')
        built = []
        for adapter in adapters:
            if isinstance(adapter, MessageBusConfig):
                built.append(adapter)
            elif isinstance(adapter, dict):
                built.append(config_class(**adapter))
            else:
                raise ValueError(f'Adapter entries must be mappings, got {type(adapter).__name__}.')
        data['adapters'] = built
        return data

    def find_adapter(self, unique_remote_id: tuple | list) -> MessageBusConfig | None:
        """Return the adapter configuration whose unique_remote_id matches, if any."""
        wanted = tuple(unique_remote_id)
        for adapter in self.adapters:
            if adapter.unique_remote_id(self.bus_type) == wanted:
                return adapter
        return None

    def remote_ids(self) -> list[tuple]:
        return [adapter.unique_remote_id(self.bus_type) for adapter in self.adapters]
