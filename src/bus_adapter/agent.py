# -*- coding: utf-8 -*- {{{
# ===----------------------------------------------------------------------===
#
#                 Installable Component of Eclipse VOLTTRON
#
# ===----------------------------------------------------------------------===
#
# Copyright 2025 Battelle Memorial Institute
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

import json
import logging
import sys

from pydantic import ValidationError

from volttron.client import Agent
from volttron.client.messaging.health import STATUS_BAD
from volttron.utils import load_config, vip_main

from protocol_proxy.ipc import callback, ProtocolHeaders, ProtocolProxyMessage
from protocol_proxy.manager.gevent import GeventProtocolProxyManager

from .config import MessageBusAdapterConfig

logging.basicConfig(filename='bus_adapter.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)
__version__ = '0.1.0'


class MessageBusAdapter(Agent):
    def __init__(self, config_path, **kwargs):
        super().__init__(**kwargs)
        self.config: MessageBusAdapterConfig = self._load_agent_config(load_config(config_path) if config_path else {})
        self.ppm = GeventProtocolProxyManager
        self.ppm.register_callback(self.handle_publish_local, 'PUBLISH_LOCAL')

    #########################
    # Configuration & Startup
    #########################

    def _load_agent_config(self, config: dict):
        try:
            return MessageBusAdapterConfig(**config)
        except ValidationError as e:
            _log.warning(f'Validation of platform driver configuration file failed. Using default values. --- {str(e)}')
            if self.core.connected:  # TODO: Is this a valid way to make sure we are ready to call subsystems?
                self.vip.health.set_status(STATUS_BAD, f'Error processing configuration: {e}')
            return MessageBusAdapterConfig()

    def configure_main(self, _, action: str, contents: dict):
        old_config = self.config.copy()
        new_config = self._load_agent_config(contents)
        if action == 'NEW':
            self.config = new_config
            for bus in self.config.adapters:
                manager = self.ppm.get_manager(bus.type)
                # TODO: Figure out what is in the adapters list.
                #          - This is probably a list of individual target brokers/servers.
                #          - It may be better to use get_proxy to get the peer rather than the manager here.
                # TODO: Figure out how to know to call start on each manager instance.
        else:
            pass

    @callback
    def handle_publish_local(self,  headers: ProtocolHeaders, raw_message: bytes):
        message = json.loads(raw_message.decode('utf8'))
        _log.debug(f"RECEIVED PUBLISH MESSAGE FROM REMOTE: \n\tTOPIC: {message['topic']}\n\tMESSAGE: {message['payload']}")
        # TODO: Implement local publishes.

    @callback
    def handle_subscribe_local(self,  headers: ProtocolHeaders, raw_message: bytes):
        message = json.loads(raw_message.decode('utf8'))
        _log.debug(f"RECEIVED SUBSCRIPTION REQUEST FROM REMOTE: \n\tTOPIC: {message['topic']}\n\tMESSAGE: {message['payload']}")
        # TODO: Implement local subscriptions.

    def subscribe(self, peer, topics):
        # _log.debug('MBA: IN SUBSCRIBE.')
        message = ProtocolProxyMessage(
            method_name='SUBSCRIBE_REMOTE',
            payload=json.dumps({'topics': topics}).encode('utf8')
        )
        self.ppm.send(self.ppm.peers[peer], message)  # TODO: We probably need to call get_manager first.
        # _log.debug('MBA: SUBSCRIBE COMPLETED.')

    def publish(self, peer, topic, payload):
        # _log.debug('MBA: IN PUBLISH.')
        message = ProtocolProxyMessage(
            method_name='PUBLISH_REMOTE',
            payload=json.dumps({'topic': topic, 'payload': payload}).encode('utf8')
        )
        self.ppm.send(self.ppm.peers[peer], message)  # TODO: We probably need to call get_manager first.

def main():
    """Main method called to start the agent."""
    vip_main(MessageBusAdapter, identity='platform.bus_adapter', version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass