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

from functools import partial
from pydantic import ValidationError
from typing import Type, Callable

from volttron.client import Agent
from volttron.client.messaging.health import STATUS_BAD
from volttron.client.vip.agent import RPC
from volttron.utils import load_config, vip_main

from interoperability.resource import ResourceData

from protocol_proxy.ipc import callback, ProtocolHeaders, ProtocolProxyMessage, ProtocolProxyPeer
from protocol_proxy.manager.gevent import GeventProtocolProxyManager

from .config import MessageBusAdapterConfig

_log = logging.getLogger(__name__)
__version__ = '0.1.0'


class MessageBusAdapter(Agent):
    manager_callbacks: tuple[
        tuple[Callable[[ProtocolHeaders, bytes], None], str], tuple[Callable[[ProtocolHeaders, bytes], None], str]]

    def __init__(self, config_path, **kwargs):
        super().__init__(**kwargs)
        self.config: MessageBusAdapterConfig = MessageBusAdapterConfig(bus_type='', adapters=[])
        self.ppm: Type[GeventProtocolProxyManager] | GeventProtocolProxyManager = GeventProtocolProxyManager
        self.manager_callbacks = ((self.handle_publish_local, 'PUBLISH_LOCAL'),
                                  (self.handle_subscribe_local, 'SUBSCRIBE_LOCAL'))
        self.resources: dict[str, ResourceData] = {}
        self.vip.config.subscribe(self.configure_main, ['NEW'], 'config')

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
            return MessageBusAdapterConfig(bus_type='', adapters=[])

    def configure_main(self, _, action: str, contents: dict):
        old_config = self.config.model_copy()  # TODO: deep=True?
        new_config = self._load_agent_config(contents)
        if action == 'NEW':
            self.config = new_config
            self.ppm: GeventProtocolProxyManager = self.ppm.get_manager(self.config.bus_type)
            for manager_callback in self.manager_callbacks:
                self.ppm.register_callback(*manager_callback)
            self.ppm.start()
            self.core.spawn(self.ppm.select_loop)
            # for bus in self.config.adapters:
            #     # TODO: Should proxies be started here, or on demand later?
        else:
            pass

    def _get_resource_data(self, message, headers):
        if not (data_resource := self.resources.get(message['topic'])):
            if data_resource := ResourceData.lookup(self, message['topic'], self._get_delimiter(headers)):
                self.resources[message['topic']] = data_resource
            else:
                _log.warning(f'Unable to find Data Resource matching {message["topic"]}')
        return data_resource

    @callback
    def handle_publish_local(self,  headers: ProtocolHeaders, raw_message: bytes):
        message = json.loads(raw_message.decode('utf8'))
        _log.debug(f"RECEIVED PUBLISH MESSAGE FROM REMOTE: \n\tTOPIC: {message['topic']}\n\tMESSAGE: {message['payload']}")
        if resource_data := self._get_resource_data(message, headers):
            transformed_payload = resource_data.transform(message['payload'])
            self.vip.pubsub.publish('pubsub', topic=resource_data.local_topic, message=transformed_payload)

    @callback
    def handle_subscribe_local(self,  headers: ProtocolHeaders, raw_message: bytes):
        message = json.loads(raw_message.decode('utf8'))
        _log.debug(f"RECEIVED SUBSCRIPTION REQUEST FROM REMOTE: \n\tTOPIC: {message['topic']}\n\tMESSAGE: {message['payload']}")
        if resource_data := self._get_resource_data(message, headers):
            manager, peer = self.ppm.get_by_proxy_id(headers.sender_id)
            if not manager or not peer:
                _log.warning(f"Incoming subscription request didn't find return path to sender: {headers.sender_id}")
            resource_data.subscribe(partial(self._publish_remote, manager=manager, peer=peer, topic=message['topic']))

    @RPC.export
    def subscribe(self, unique_remote_id: tuple, topics: str):
        # _log.debug('MBA: IN SUBSCRIBE.'):
        message = ProtocolProxyMessage(
            method_name='SUBSCRIBE_REMOTE',
            payload=json.dumps({'topics': topics}).encode('utf8')
        )
        manager, peer = self.ppm.get_proxy(unique_remote_id=unique_remote_id, manager_callbacks=self.manager_callbacks)
        manager.send(remote=peer, message=message)
        # _log.debug('MBA: SUBSCRIBE COMPLETED.')

    def handle_subscription_request(self, peer, sender, bus, topic, headers, message):
        pass

    @RPC.export
    def publish(self, unique_remote_id: tuple, topic: str, payload, transform_key=None):
        # _log.debug('MBA: IN PUBLISH.')
        # TODO: Transform_key is no longer used. _publish_remote now expects a transform function.
        manager, peer = self.ppm.get_proxy(unique_remote_id=unique_remote_id, manager_callbacks=self.manager_callbacks)
        self._publish_remote(manager, peer, topic, payload) #, transform)

    @staticmethod
    def _publish_remote(manager: GeventProtocolProxyManager, peer: ProtocolProxyPeer,
                        topic: str, payload):
        message = ProtocolProxyMessage(
            method_name='PUBLISH_REMOTE',
            payload=json.dumps({'topic': topic, 'payload': payload}).encode('utf8')
        )
        manager.send(remote=peer, message=message)

    def handle_publish_request(self, peer, sender, bus, topic, headers, message):
        pass

    def _get_delimiter(self, headers: ProtocolHeaders) -> str | None:
        manager, _ = self.ppm.get_by_proxy_id(headers.sender_id)
        delimiter = manager.proxy_class.topic_delimiter() if hasattr(manager.proxy_class, 'topic_delimiter') else None
        return delimiter

def main():
    """Main method called to start the agent."""
    vip_main(MessageBusAdapter, identity='platform.bus_adapter', version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass