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

from importlib import import_module
from typing import Any, Callable, Type
from uuid import UUID

from pydantic import ValidationError

from volttron.client import Agent
from volttron.client.messaging.health import STATUS_BAD, STATUS_GOOD
from volttron.client.vip.agent import Core, RPC
from volttron.utils import load_config, vip_main

from interoperability.resource import ResourceData

from protocol_proxy.ipc import callback, ProtocolHeaders, ProtocolProxyMessage, ProtocolProxyPeer
from protocol_proxy.manager.gevent import GeventProtocolProxyManager
from protocol_proxy.proxy import ProtocolProxy

from .config import MessageBusAdapterConfig

_log = logging.getLogger(__name__)
__version__ = '2.0.0rc0'

DEFAULT_TOPIC_DELIMITER = '/'


class MessageBusAdapter(Agent):
    """Relays publications and subscriptions between VOLTTRON and a foreign message bus.

    One ProtocolProxyManager is created for the configured ``bus_type``. Each entry in
    ``adapters`` describes one remote bus and is run in its own proxy subprocess.

    Message flow:
      remote -> VOLTTRON: the proxy sends PUBLISH_LOCAL; the topic is resolved to a
        canonical resource through platform.presentation, decoded if the alias declares
        a binary encoding, transformed, and published.
      VOLTTRON -> remote: a remote topic listed in the adapter's ``local_subscriptions``
        (or sent by the proxy as SUBSCRIBE_LOCAL) is resolved the same way; matching local
        publications are transformed, encoded and forwarded to the proxy as PUBLISH_REMOTE.
        Binary payloads travel hex-encoded with ``"encoding": "hex"`` in the envelope.
      A peer may also call the ``subscribe``/``publish`` RPCs.
    """

    def __init__(self, config_path: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.config: MessageBusAdapterConfig = MessageBusAdapterConfig()
        self.ppm: GeventProtocolProxyManager | None = None
        self._select_loop = None
        self.manager_callbacks: tuple[tuple[Callable[[ProtocolHeaders, bytes], None], str], ...] = (
            (self.handle_publish_local, 'PUBLISH_LOCAL'),
            (self.handle_subscribe_local, 'SUBSCRIBE_LOCAL'),
        )
        self.resources: dict[str, ResourceData] = {}
        self._remote_subscriptions: set[tuple[UUID, str]] = set()
        if config_path:
            self.vip.config.set_default('config', load_config(config_path))
        self.vip.config.subscribe(self.configure_main, ['NEW', 'UPDATE'], 'config')

    #########################
    # Configuration & Startup
    #########################

    def _load_agent_config(self, contents: dict) -> MessageBusAdapterConfig | None:
        try:
            return MessageBusAdapterConfig(**contents)
        except (ValidationError, TypeError, ValueError) as e:
            self._report_bad_status(f'Validation of message bus adapter configuration failed: {e}')
            return None

    def _report_bad_status(self, message: str):
        _log.error(message)
        if self.core.connected:    # TODO: Is this a valid way to make sure we are ready to call subsystems?
            self.vip.health.set_status(STATUS_BAD, message)

    def configure_main(self, _, action: str, contents: dict):
        new_config = self._load_agent_config(contents)
        if new_config is None:
            return    # Keep the previous configuration.
        if self.ppm is not None and new_config.bus_type != self.config.bus_type:
            self._report_bad_status(f'Changing bus_type at runtime ({self.config.bus_type} -> {new_config.bus_type})'
                                    ' is not supported. Restart the agent. Keeping the existing configuration.')
            return
        self.config = new_config
        if not self.config.bus_type:
            _log.warning('No bus_type configured. The Message Bus Adapter is idle until a configuration is provided.')
            return
        try:
            if self.ppm is None:
                self._start_manager(self.config.bus_type)
        except (ImportError, ValueError, OSError) as e:
            self._report_bad_status(f'Unable to start proxy manager for bus_type "{self.config.bus_type}": {e}')
            return
        # Launch a proxy for each configured remote without blocking the config handler.
        for unique_remote_id in self.config.remote_ids():
            self.core.spawn(self._start_remote, unique_remote_id)
        if self.core.connected:
            self.vip.health.set_status(STATUS_GOOD, f'Configured for bus_type "{self.config.bus_type}".')

    def _start_manager(self, bus_type: str):
        proxy_class = self._resolve_proxy_class(bus_type)
        self.ppm = GeventProtocolProxyManager.get_manager(proxy_class, self.manager_callbacks)
        # get_manager only registers callbacks when it creates the manager; registering is idempotent.
        for manager_callback in self.manager_callbacks:
            self.ppm.register_callback(*manager_callback)
        self.ppm.start()
        self._select_loop = self.core.spawn(self.ppm.select_loop)

    @staticmethod
    def _resolve_proxy_class(bus_type: str) -> Type[ProtocolProxy]:
        """Find the ProtocolProxy subclass for a bus type.

        Prefers the plugin's PROXY_CLASS attribute (as ProtocolProxyManager.get_manager does)
        but falls back to the single ProtocolProxy subclass defined by the plugin package.
        """
        module_name = f'protocol_proxy.protocol.{bus_type}'
        try:
            module = import_module(module_name)
        except ImportError as e:
            raise ImportError(f'No protocol proxy plugin found for bus_type "{bus_type}" ({module_name}): {e}') from e
        proxy_class = getattr(module, 'PROXY_CLASS', None)
        if isinstance(proxy_class, type) and issubclass(proxy_class, ProtocolProxy):
            return proxy_class
        candidates = [obj for obj in vars(module).values()
                      if isinstance(obj, type) and issubclass(obj, ProtocolProxy)
                      and obj.__module__.startswith(module_name)]
        if len(candidates) == 1:
            return candidates[0]
        raise ValueError(f'{module_name} does not define PROXY_CLASS and has {len(candidates)}'
                         ' ProtocolProxy subclasses; cannot choose one.')

    def _start_remote(self, unique_remote_id: tuple):
        """Launch the proxy for a configured remote and apply its config-declared subscriptions."""
        try:
            peer = self._get_peer(unique_remote_id)
        except (ValueError, TimeoutError, RuntimeError) as e:
            _log.warning(f'Unable to start proxy for {unique_remote_id}: {e}')
            return
        adapter = self.config.find_adapter(tuple(unique_remote_id))
        if adapter is None:
            return
        if adapter.local_subscriptions:
            self._subscribe_local(self.ppm, peer, list(adapter.local_subscriptions))
        if adapter.remote_subscriptions:
            self.subscribe(unique_remote_id, list(adapter.remote_subscriptions))

    def _get_peer(self, unique_remote_id: tuple | list) -> ProtocolProxyPeer:
        """Get (launching if necessary) the registered proxy peer for a configured remote."""
        if self.ppm is None:
            raise RuntimeError('The Message Bus Adapter has not been configured with a bus_type.')
        unique_remote_id = tuple(unique_remote_id)
        adapter = self.config.find_adapter(unique_remote_id)
        if adapter is None:
            raise ValueError(f'No adapter is configured for remote {unique_remote_id}.'
                             f' Known remotes: {self.config.remote_ids()}')
        peer = self.ppm.get_proxy(unique_remote_id, **adapter.proxy_kwargs())
        if peer.socket_params is None:
            # The proxy process must register its socket before anything can be sent to it.
            self.ppm.wait_peer_registered(peer, self.config.proxy_registration_timeout)
            if peer.socket_params is None:
                raise TimeoutError(f'Proxy for {unique_remote_id} did not register within'
                                   f' {self.config.proxy_registration_timeout} seconds.')
        return peer

    @Core.receiver('onstop')
    def onstop(self, sender, **kwargs):
        if self.ppm is not None:
            self.ppm.stop()

    ###################
    # Remote -> Local
    ###################

    def _get_resource_data(self, topic: str, delimiter: str = DEFAULT_TOPIC_DELIMITER) -> ResourceData | None:
        if not (resource_data := self.resources.get(topic)):
            if resource_data := ResourceData.lookup(self, topic, delimiter):
                self.resources[topic] = resource_data
            else:
                _log.warning(f'Unable to find Data Resource matching {topic}')
        return resource_data

    @staticmethod
    def _decode_remote_payload(payload: Any) -> Any:
        """Decode payloads as sent by the MQTT and NATS proxies: hex-encoded bytes holding
        UTF-8 text, usually JSON. Anything that does not fit that shape is returned as-is."""
        if not isinstance(payload, str):
            return payload
        try:
            raw = bytes.fromhex(payload)
        except ValueError:
            return payload
        try:
            text = raw.decode('utf8')
        except UnicodeDecodeError:
            return raw
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text

    @callback
    def handle_publish_local(self, headers: ProtocolHeaders, raw_message: bytes):
        message = json.loads(raw_message.decode('utf8'))
        topic = message.get('topic')
        if not topic:
            _log.warning(f'Received PUBLISH_LOCAL without a topic from {headers.sender_id}: {message}')
            return
        payload = self._decode_remote_payload(message.get('payload'))
        _log.debug(f'RECEIVED PUBLISH MESSAGE FROM REMOTE: \n\tTOPIC: {topic}\n\tMESSAGE: {payload}')
        if resource_data := self._get_resource_data(topic, self._get_delimiter(headers)):
            # Binary payloads (protobuf) are decoded by the alias's codec; JSON has already been parsed above.
            transformed_payload = resource_data.transform.execute(resource_data.decode(payload))
            local_topic = resource_data.resource_def.get('publication_topic') or resource_data.local_topic
            self.vip.pubsub.publish('pubsub', topic=local_topic, message=transformed_payload)

    ###################
    # Local -> Remote
    ###################

    @callback
    def handle_subscribe_local(self, headers: ProtocolHeaders, raw_message: bytes):
        message = json.loads(raw_message.decode('utf8'))
        topics = message.get('topics') or ([message['topic']] if message.get('topic') else [])
        if isinstance(topics, str):
            topics = [topics]
        _log.debug(f'RECEIVED SUBSCRIPTION REQUEST FROM REMOTE: \n\tTOPICS: {topics}')
        manager, peer = GeventProtocolProxyManager.get_by_proxy_id(headers.sender_id)
        if manager is None or peer is None:
            _log.warning(f"Incoming subscription request didn't find return path to sender: {headers.sender_id}")
            return
        self._subscribe_local(manager, peer, topics)

    def _subscribe_local(self, manager: GeventProtocolProxyManager, peer: ProtocolProxyPeer, topics: list[str]):
        """Serve remote topics from local data: resolve each through platform.presentation, subscribe to the
        canonical resource's publications and relay them, transformed and encoded, to the peer. Topics already
        served to this peer are skipped, so repeated requests and configuration reloads are harmless."""
        delimiter = self._delimiter_for(manager)
        for topic in topics:
            key = (peer.proxy_id, topic)
            if key in self._remote_subscriptions:
                continue
            if resource_data := self._get_resource_data(topic, delimiter):
                resource_data.subscribe(self._make_remote_relay(manager, peer, topic))
                self._remote_subscriptions.add(key)
                _log.info(f'Serving remote topic "{topic}" from {resource_data.resource_def.get("publication_topic")}')

    def _make_remote_relay(self, manager: GeventProtocolProxyManager, peer: ProtocolProxyPeer, remote_topic: str
                           ) -> Callable:
        """Build a VIP pubsub callback that forwards (already transformed) local messages to a remote."""
        def relay_to_remote(_peer, _sender, _bus, _topic, _headers, message):
            self._publish_remote(manager, peer, remote_topic, message)
        return relay_to_remote

    @staticmethod
    def _publish_remote(manager: GeventProtocolProxyManager, peer: ProtocolProxyPeer, topic: str, payload) -> bool:
        """Send PUBLISH_REMOTE. JSON-serializable payloads go as they are; ``bytes`` (a protobuf message) are
        hex-encoded and flagged with ``"encoding": "hex"`` so the proxy publishes the raw bytes."""
        if isinstance(payload, (bytes, bytearray)):
            body = {'topic': topic, 'payload': bytes(payload).hex(), 'encoding': 'hex'}
        else:
            body = {'topic': topic, 'payload': payload}
        message = ProtocolProxyMessage(method_name='PUBLISH_REMOTE', payload=json.dumps(body).encode('utf8'))
        return bool(manager.send(remote=peer, message=message))

    ###################
    # RPC Interface
    ###################

    @RPC.export
    def list_remotes(self) -> list[list]:
        """Return the unique_remote_ids of all configured remotes."""
        return [list(remote_id) for remote_id in self.config.remote_ids()]

    @RPC.export
    def subscribe(self, unique_remote_id: tuple | list, topics: str | list[str]) -> bool:
        """Ask the remote bus identified by unique_remote_id to subscribe to topics."""
        topics = [topics] if isinstance(topics, str) else list(topics)
        peer = self._get_peer(unique_remote_id)
        message = ProtocolProxyMessage(
            method_name='SUBSCRIBE_REMOTE',
            payload=json.dumps({'topics': topics}).encode('utf8')
        )
        return bool(self.ppm.send(remote=peer, message=message))

    @RPC.export
    def publish(self, unique_remote_id: tuple | list, topic: str, payload) -> bool:
        """Publish payload to topic on the remote bus identified by unique_remote_id."""
        peer = self._get_peer(unique_remote_id)
        return self._publish_remote(self.ppm, peer, topic, payload)

    ###################
    # Helpers
    ###################

    @staticmethod
    def _delimiter_for(manager) -> str:
        """The topic delimiter of a manager's proxy class (``/`` for MQTT, ``.`` for NATS)."""
        delimiter_func = getattr(getattr(manager, 'proxy_class', None), 'topic_delimiter', None)
        return delimiter_func() if callable(delimiter_func) else DEFAULT_TOPIC_DELIMITER

    @classmethod
    def _get_delimiter(cls, headers: ProtocolHeaders) -> str:
        manager, _ = GeventProtocolProxyManager.get_by_proxy_id(headers.sender_id)
        return cls._delimiter_for(manager)


def main():
    """Main method called to start the agent."""
    vip_main(MessageBusAdapter, identity='platform.bus_adapter', version=__version__)


if __name__ == '__main__':
    # Entry point for script
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
