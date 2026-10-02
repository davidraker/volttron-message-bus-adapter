import json
from unittest import mock
from uuid import uuid4

import pytest

from bus_adapter import agent as agent_module
from bus_adapter.agent import MessageBusAdapter
from bus_adapter.config import MessageBusAdapterConfig
from protocol_proxy.ipc import SocketParams
from protocol_proxy.manager.gevent import GeventProtocolProxyManager


class FakePeer:
    def __init__(self, registered=True):
        self.proxy_id = uuid4()
        self.token = uuid4()
        self.socket_params = SocketParams('127.0.0.1', 1) if registered else None


class FakeHeaders:
    def __init__(self, sender_id, sender_token):
        self.sender_id = sender_id
        self.sender_token = sender_token


@pytest.fixture
def adapter():
    """A MessageBusAdapter with VOLTTRON's Agent machinery stubbed out."""
    with mock.patch.object(agent_module.Agent, '__init__', return_value=None):
        a = MessageBusAdapter.__new__(MessageBusAdapter)
        a.vip = mock.MagicMock()
        a.core = mock.MagicMock()
        a.core.connected = True
        MessageBusAdapter.__init__(a, config_path=None)
    a.config = MessageBusAdapterConfig(bus_type='mqtt', adapters=[{'name': 'r1', 'host': 'h', 'port': 1}])
    a.ppm = mock.MagicMock(spec=GeventProtocolProxyManager)
    a.ppm.send.return_value = True
    return a


def test_get_peer_passes_only_proxy_kwargs_and_waits(adapter):
    peer = FakePeer(registered=False)

    def register(p, timeout):
        p.socket_params = SocketParams('127.0.0.1', 5)
    adapter.ppm.get_proxy.return_value = peer
    adapter.ppm.wait_peer_registered.side_effect = register

    assert adapter._get_peer(['mqtt', 'r1']) is peer
    adapter.ppm.get_proxy.assert_called_once_with(('mqtt', 'r1'), host='h', port=1, keepalive=60,
                                                  bind_address='', bind_port=0)
    adapter.ppm.wait_peer_registered.assert_called_once_with(peer, 30.0)


def test_get_peer_unknown_remote_and_timeout(adapter):
    with pytest.raises(ValueError):
        adapter._get_peer(('mqtt', 'unknown'))
    adapter.ppm.get_proxy.return_value = FakePeer(registered=False)
    with pytest.raises(TimeoutError):
        adapter._get_peer(('mqtt', 'r1'))


def test_get_peer_before_configuration():
    with mock.patch.object(agent_module.Agent, '__init__', return_value=None):
        a = MessageBusAdapter.__new__(MessageBusAdapter)
        a.vip = mock.MagicMock()
        a.core = mock.MagicMock()
        MessageBusAdapter.__init__(a)
    with pytest.raises(RuntimeError):
        a._get_peer(('mqtt', 'x'))


def test_subscribe_and_publish_rpc(adapter):
    peer = FakePeer()
    adapter.ppm.get_proxy.return_value = peer

    assert adapter.subscribe(['mqtt', 'r1'], 'a/b') is True
    message = adapter.ppm.send.call_args.kwargs['message']
    assert message.method_name == 'SUBSCRIBE_REMOTE'
    assert json.loads(message.payload) == {'topics': ['a/b']}

    assert adapter.publish(('mqtt', 'r1'), 't', {'v': 1}) is True
    message = adapter.ppm.send.call_args.kwargs['message']
    assert message.method_name == 'PUBLISH_REMOTE'
    assert json.loads(message.payload) == {'topic': 't', 'payload': {'v': 1}}
    assert adapter.list_remotes() == [['mqtt', 'r1']]


def test_decode_remote_payload():
    decode = MessageBusAdapter._decode_remote_payload
    assert decode(json.dumps({'a': 1}).encode().hex()) == {'a': 1}
    assert decode(b'plain text'.hex()) == 'plain text'
    assert decode('not hex!') == 'not hex!'
    assert decode({'already': 'decoded'}) == {'already': 'decoded'}
    assert decode(b'\xff\xfe'.hex()) == b'\xff\xfe'


def _remote(adapter, manager):
    peer = FakePeer()
    manager.peers = {peer.proxy_id: peer}
    manager.proxy_class = mock.MagicMock(topic_delimiter=mock.Mock(return_value='/'))
    headers = FakeHeaders(peer.proxy_id, peer.token)
    return peer, headers


def test_handle_publish_local_transforms_and_publishes(adapter):
    manager = adapter.ppm
    peer, headers = _remote(adapter, manager)
    resource = mock.MagicMock()
    resource.decode.side_effect = lambda payload: payload       # a JSON alias: nothing to decode
    resource.transform.execute.return_value = {'out': 1}
    resource.resource_def = {'publication_topic': 'devices/x'}
    resource.local_topic = 'a/b/c'
    with mock.patch.object(GeventProtocolProxyManager, 'get_by_proxy_id', return_value=(manager, peer)), \
         mock.patch.object(agent_module.ResourceData, 'lookup', return_value=resource) as lookup:
        raw = json.dumps({'topic': 'a/b/c', 'payload': json.dumps({'in': 1}).encode().hex()}).encode()
        adapter.handle_publish_local(manager, headers, raw)
        adapter.handle_publish_local(manager, headers, raw)    # second call uses the cache
    lookup.assert_called_once_with(adapter, 'a/b/c', '/')
    resource.transform.execute.assert_called_with({'in': 1})
    adapter.vip.pubsub.publish.assert_called_with('pubsub', topic='devices/x', message={'out': 1})


def test_handle_publish_local_rejects_unauthenticated_sender(adapter):
    manager = adapter.ppm
    peer, headers = _remote(adapter, manager)
    headers.sender_token = uuid4()
    with mock.patch.object(agent_module.ResourceData, 'lookup') as lookup:
        adapter.handle_publish_local(manager, headers, json.dumps({'topic': 't', 'payload': ''}).encode())
    lookup.assert_not_called()


def test_handle_subscribe_local_relays_with_pubsub_signature(adapter):
    manager = adapter.ppm
    peer, headers = _remote(adapter, manager)
    resource = mock.MagicMock()
    with mock.patch.object(GeventProtocolProxyManager, 'get_by_proxy_id', return_value=(manager, peer)), \
         mock.patch.object(agent_module.ResourceData, 'lookup', return_value=resource):
        raw = json.dumps({'topic': 'remote/topic'}).encode()
        adapter.handle_subscribe_local(manager, headers, raw)
        adapter.handle_subscribe_local(manager, headers, raw)    # duplicate is ignored
    resource.subscribe.assert_called_once()
    relay = resource.subscribe.call_args.args[0]
    # ResourceData.subscribe invokes the callback with the VIP pubsub signature.
    relay('pubsub', 'sender', 'bus', 'remote/topic', {}, {'transformed': True})
    manager.send.assert_called_once()
    sent = manager.send.call_args.kwargs
    assert sent['remote'] is peer
    assert json.loads(sent['message'].payload) == {'topic': 'remote/topic', 'payload': {'transformed': True}}


def test_handle_subscribe_local_without_return_path(adapter):
    manager = adapter.ppm
    peer, headers = _remote(adapter, manager)
    with mock.patch.object(GeventProtocolProxyManager, 'get_by_proxy_id', return_value=(None, None)), \
         mock.patch.object(agent_module.ResourceData, 'lookup') as lookup:
        adapter.handle_subscribe_local(manager, headers, json.dumps({'topics': ['x']}).encode())
    lookup.assert_not_called()


def test_get_delimiter_defaults_to_slash():
    headers = FakeHeaders(uuid4(), uuid4())
    with mock.patch.object(GeventProtocolProxyManager, 'get_by_proxy_id', return_value=(None, None)):
        assert MessageBusAdapter._get_delimiter(headers) == '/'
    manager = mock.MagicMock()
    manager.proxy_class = mock.MagicMock(spec=[])    # no topic_delimiter attribute
    with mock.patch.object(GeventProtocolProxyManager, 'get_by_proxy_id', return_value=(manager, object())):
        assert MessageBusAdapter._get_delimiter(headers) == '/'


def test_configure_main_invalid_config_keeps_previous(adapter):
    previous = adapter.config
    adapter.configure_main(None, 'NEW', {'bus_type': 'mqtt', 'adapters': [{'port': 1}]})
    assert adapter.config is previous
    adapter.vip.health.set_status.assert_called_once()
    assert adapter.vip.health.set_status.call_args.args[0] == agent_module.STATUS_BAD


def test_configure_main_rejects_bus_type_change(adapter):
    adapter.configure_main(None, 'UPDATE', {'bus_type': 'nats', 'adapters': []})
    assert adapter.config.bus_type == 'mqtt'


def test_configure_main_starts_manager_and_remotes():
    with mock.patch.object(agent_module.Agent, '__init__', return_value=None):
        a = MessageBusAdapter.__new__(MessageBusAdapter)
        a.vip = mock.MagicMock()
        a.core = mock.MagicMock()
        a.core.connected = True
        MessageBusAdapter.__init__(a)
    manager = mock.MagicMock(spec=GeventProtocolProxyManager)
    proxy_class = object()
    with mock.patch.object(MessageBusAdapter, '_resolve_proxy_class', return_value=proxy_class), \
         mock.patch.object(GeventProtocolProxyManager, 'get_manager', return_value=manager) as get_manager:
        a.configure_main(None, 'NEW', {'bus_type': 'mqtt', 'adapters': [{'host': 'h'}]})
    get_manager.assert_called_once_with(proxy_class, a.manager_callbacks)
    manager.start.assert_called_once()
    a.core.spawn.assert_any_call(manager.select_loop)
    a.core.spawn.assert_any_call(a._start_remote, ('mqtt', 'h', 1883))
    assert a.vip.health.set_status.call_args.args[0] == agent_module.STATUS_GOOD


def test_configure_main_import_error_sets_bad_status():
    with mock.patch.object(agent_module.Agent, '__init__', return_value=None):
        a = MessageBusAdapter.__new__(MessageBusAdapter)
        a.vip = mock.MagicMock()
        a.core = mock.MagicMock()
        a.core.connected = True
        MessageBusAdapter.__init__(a)
    a.configure_main(None, 'NEW', {'bus_type': 'no_such_bus', 'adapters': []})
    assert a.ppm is None
    assert a.vip.health.set_status.call_args.args[0] == agent_module.STATUS_BAD


def test_resolve_proxy_class_fallback_without_PROXY_CLASS():
    import types
    from protocol_proxy.proxy import ProtocolProxy

    class FakeProxy(ProtocolProxy):
        pass
    FakeProxy.__module__ = 'protocol_proxy.protocol.fake.fake_proxy'
    module = types.ModuleType('protocol_proxy.protocol.fake')
    module.FakeProxy = FakeProxy
    with mock.patch.dict('sys.modules', {'protocol_proxy.protocol.fake': module}):
        assert MessageBusAdapter._resolve_proxy_class('fake') is FakeProxy
    module.PROXY_CLASS = FakeProxy
    with mock.patch.dict('sys.modules', {'protocol_proxy.protocol.fake': module}):
        assert MessageBusAdapter._resolve_proxy_class('fake') is FakeProxy


def test_start_remote_applies_configured_subscriptions(adapter):
    adapter.config = MessageBusAdapterConfig(bus_type='mqtt', adapters=[
        {'name': 'r1', 'host': 'h', 'port': 1, 'local_subscriptions': ['openfmb/a', 'openfmb/b'],
         'remote_subscriptions': ['openfmb/c']}])
    peer = FakePeer()
    with mock.patch.object(adapter, '_get_peer', return_value=peer), \
         mock.patch.object(adapter, '_subscribe_local') as subscribe_local, \
         mock.patch.object(adapter, 'subscribe') as subscribe:
        adapter._start_remote(('mqtt', 'r1'))
    subscribe_local.assert_called_once_with(adapter.ppm, peer, ['openfmb/a', 'openfmb/b'])
    subscribe.assert_called_once_with(('mqtt', 'r1'), ['openfmb/c'])
    # Nothing declared: nothing sent. A proxy that fails to start is only logged.
    adapter.config = MessageBusAdapterConfig(bus_type='mqtt', adapters=[{'name': 'r1', 'host': 'h', 'port': 1}])
    with mock.patch.object(adapter, '_get_peer', return_value=peer), \
         mock.patch.object(adapter, '_subscribe_local') as subscribe_local:
        adapter._start_remote(('mqtt', 'r1'))
    subscribe_local.assert_not_called()
    with mock.patch.object(adapter, '_get_peer', side_effect=TimeoutError('late')):
        adapter._start_remote(('mqtt', 'r1'))


def test_subscribe_local_resolves_with_the_proxy_delimiter_and_relays(adapter):
    manager = adapter.ppm
    peer, _ = _remote(adapter, manager)
    manager.proxy_class = mock.MagicMock(topic_delimiter=mock.Mock(return_value='.'))
    resource = mock.MagicMock()
    resource.resource_def = {'publication_topic': 'devices/ess/all'}
    with mock.patch.object(agent_module.ResourceData, 'lookup', return_value=resource) as lookup:
        adapter._subscribe_local(manager, peer, ['openfmb.essmodule.ESSReadingProfile.m1'])
        adapter._subscribe_local(manager, peer, ['openfmb.essmodule.ESSReadingProfile.m1'])    # already served
    lookup.assert_called_once_with(adapter, 'openfmb.essmodule.ESSReadingProfile.m1', '.')
    resource.subscribe.assert_called_once()
    relay = resource.subscribe.call_args.args[0]
    relay('pubsub', 'sender', 'bus', 'devices/ess/all', {}, b'\x0a\x02\x08\x01')      # ResourceData already encoded it
    sent = json.loads(manager.send.call_args.kwargs['message'].payload)
    assert sent == {'topic': 'openfmb.essmodule.ESSReadingProfile.m1', 'payload': '0a020801', 'encoding': 'hex'}


def test_publish_remote_envelopes(adapter):
    peer = FakePeer()
    MessageBusAdapter._publish_remote(adapter.ppm, peer, 't', {'v': 1})
    assert json.loads(adapter.ppm.send.call_args.kwargs['message'].payload) == {'topic': 't', 'payload': {'v': 1}}
    MessageBusAdapter._publish_remote(adapter.ppm, peer, 't', bytearray(b'\xff\x00'))
    assert json.loads(adapter.ppm.send.call_args.kwargs['message'].payload) == {'topic': 't', 'payload': 'ff00', 'encoding': 'hex'}


def test_handle_publish_local_decodes_binary_payloads(adapter):
    manager = adapter.ppm
    peer, headers = _remote(adapter, manager)
    resource = mock.MagicMock()
    resource.decode.side_effect = lambda payload: {'decoded': payload.hex()} if isinstance(payload, bytes) else payload
    resource.transform.execute.side_effect = lambda payload: payload
    resource.resource_def = {'publication_topic': 'devices/x'}
    with mock.patch.object(GeventProtocolProxyManager, 'get_by_proxy_id', return_value=(manager, peer)), \
         mock.patch.object(agent_module.ResourceData, 'lookup', return_value=resource):
        raw = json.dumps({'topic': 'a/b/c', 'payload': b'\xff\xfe'.hex()}).encode()      # not UTF-8: stays bytes
        adapter.handle_publish_local(manager, headers, raw)
    adapter.vip.pubsub.publish.assert_called_with('pubsub', topic='devices/x', message={'decoded': 'fffe'})
