import json
from unittest import mock
from uuid import uuid4

import pytest

from icm.resources.openfmb import bus as openfmb_bus

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


def test_handle_publish_local_republishes_on_the_topic_and_caches_the_lookup(adapter):
    manager = adapter.ppm
    peer, headers = _remote(adapter, manager)
    resource = mock.MagicMock()
    resource.decode.side_effect = lambda payload: payload       # a JSON alias: nothing to decode
    resource.resource_def = {'codec': {'proto': 'solarmodule.SolarReadingProfile'}}
    with mock.patch.object(GeventProtocolProxyManager, 'get_by_proxy_id', return_value=(manager, peer)), \
         mock.patch.object(agent_module.ResourceData, 'lookup', return_value=resource) as lookup:
        raw = json.dumps({'topic': 'a/b/c', 'payload': json.dumps({'in': 1}).encode().hex()}).encode()
        adapter.handle_publish_local(manager, headers, raw)
        adapter.handle_publish_local(manager, headers, raw)    # second call uses the cache
    lookup.assert_called_once_with(adapter, 'a/b/c', '/', strict=True)    # inbound: the exact topic, no prefix fallback
    assert adapter.vip.pubsub.publish.call_count == 2
    call = adapter.vip.pubsub.publish.call_args
    assert call.kwargs['topic'] == 'a/b/c' and call.kwargs['message'] == {'in': 1}
    assert openfmb_bus.origin_of(call.kwargs['headers']) == adapter.identity   # so its own republication is not sent back

def test_handle_publish_local_rejects_unauthenticated_sender(adapter):
    manager = adapter.ppm
    peer, headers = _remote(adapter, manager)
    headers.sender_token = uuid4()
    with mock.patch.object(agent_module.ResourceData, 'lookup') as lookup:
        adapter.handle_publish_local(manager, headers, json.dumps({'topic': 't', 'payload': ''}).encode())
    lookup.assert_not_called()


def test_handle_subscribe_local_relays_local_publications_to_the_remote(adapter):
    manager = adapter.ppm
    peer, headers = _remote(adapter, manager)
    resource = mock.MagicMock()
    resource.encode.side_effect = lambda message: message
    with mock.patch.object(GeventProtocolProxyManager, 'get_by_proxy_id', return_value=(manager, peer)), \
         mock.patch.object(agent_module.ResourceData, 'lookup', return_value=resource):
        raw = json.dumps({'topic': 'remote/topic'}).encode()
        adapter.handle_subscribe_local(manager, headers, raw)
        adapter.handle_subscribe_local(manager, headers, raw)    # duplicate is ignored
    adapter.vip.pubsub.subscribe.assert_called_once()
    assert adapter.vip.pubsub.subscribe.call_args.kwargs['prefix'] == 'remote/topic'
    relay = adapter.vip.pubsub.subscribe.call_args.kwargs['callback']
    relay('pubsub', 'sender', 'bus', 'remote/topic', {}, {'local': True})           # another agent's publication
    manager.send.assert_called_once()
    sent = manager.send.call_args.kwargs
    assert sent['remote'] is peer
    assert json.loads(sent['message'].payload) == {'topic': 'remote/topic', 'payload': {'local': True}}

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
    resource.encode.side_effect = lambda message: b'\x0a\x02\x08\x01'          # the alias's protobuf codec
    with mock.patch.object(agent_module.ResourceData, 'lookup', return_value=resource) as lookup:
        adapter._subscribe_local(manager, peer, ['openfmb.essmodule.ESSReadingProfile.m1'])
        adapter._subscribe_local(manager, peer, ['openfmb.essmodule.ESSReadingProfile.m1'])    # already served
    lookup.assert_called_once_with(adapter, 'openfmb.essmodule.ESSReadingProfile.m1', '.', strict=False)   # outbound keeps the prefix fallback
    adapter.vip.pubsub.subscribe.assert_called_once()
    local_topic = adapter.vip.pubsub.subscribe.call_args.kwargs['prefix']
    assert local_topic == 'openfmb/essmodule/ESSReadingProfile/m1'                  # the remote's delimiter becomes the bus's
    relay = adapter.vip.pubsub.subscribe.call_args.kwargs['callback']
    relay('pubsub', 'sender', 'bus', local_topic, {}, {'essReading': {}})
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
    resource.resource_def = {'codec': {'proto': 'essmodule.ESSReadingProfile'}}
    with mock.patch.object(GeventProtocolProxyManager, 'get_by_proxy_id', return_value=(manager, peer)), \
         mock.patch.object(agent_module.ResourceData, 'lookup', return_value=resource):
        raw = json.dumps({'topic': 'a/b/c', 'payload': b'\xff\xfe'.hex()}).encode()      # not UTF-8: stays bytes
        adapter.handle_publish_local(manager, headers, raw)
    call = adapter.vip.pubsub.publish.call_args
    assert call.kwargs['topic'] == 'a/b/c' and call.kwargs['message'] == {'decoded': 'fffe'}
    assert call.kwargs['headers']['Content-Type'] == 'application/openfmb+json'

# ---- OpenFMB on the VOLTTRON bus (relay, the only mode) -------------------------------------------------------------------------

def test_relay_mode_republishes_inbound_messages_on_their_own_topic_with_headers(adapter):
    from icm.resources.openfmb import bus as openfmb_bus
    adapter.config = MessageBusAdapterConfig(bus_type='mqtt', adapters=[{'name': 'r1', 'host': 'h', 'port': 1}])
    adapter.core.identity = 'platform.bus_adapter'
    manager = adapter.ppm
    peer, headers = _remote(adapter, manager)
    peer.proxy_name = "('mqtt', 'r1')"
    resource = mock.MagicMock()
    resource.decode.side_effect = lambda payload: {'decoded': payload.hex()} if isinstance(payload, bytes) else payload
    resource.resource_def = {'rpc_topic': 'devices/x', 'codec': {'encoding': 'protobuf', 'proto': 'essmodule.ESSControlProfile'}}
    with mock.patch.object(GeventProtocolProxyManager, 'get_by_proxy_id', return_value=(manager, peer)), \
         mock.patch.object(agent_module.ResourceData, 'lookup', return_value=resource):
        raw = json.dumps({'topic': 'openfmb/essmodule/ESSControlProfile/m1', 'payload': b'\xff\xfe'.hex()}).encode()
        adapter.handle_publish_local(manager, headers, raw)
    adapter.vip.rpc.call.assert_not_called()
    resource.transform.execute.assert_not_called()
    call = adapter.vip.pubsub.publish.call_args
    assert call.kwargs['topic'] == 'openfmb/essmodule/ESSControlProfile/m1' and call.kwargs['message'] == {'decoded': 'fffe'}
    assert openfmb_bus.is_openfmb(call.kwargs['headers']) and openfmb_bus.proto_of(call.kwargs['headers']) == 'essmodule.ESSControlProfile'
    assert openfmb_bus.origin_of(call.kwargs['headers']) == 'platform.bus_adapter' and call.kwargs['headers'][openfmb_bus.HEADER_REMOTE] == "('mqtt', 'r1')"


def test_relay_mode_forwards_local_publications_except_its_own(adapter):
    from icm.resources.openfmb import bus as openfmb_bus
    adapter.config = MessageBusAdapterConfig(bus_type='nats', adapters=[{'name': 'r1', 'servers': 'nats://x:4222'}])
    adapter.core.identity = 'platform.bus_adapter'
    manager = adapter.ppm
    peer, _ = _remote(adapter, manager)
    manager.proxy_class = mock.MagicMock(topic_delimiter=mock.Mock(return_value='.'))
    resource = mock.MagicMock()
    resource.encode.side_effect = lambda message: b'\x0a\x02\x08\x01'
    with mock.patch.object(agent_module.ResourceData, 'lookup', return_value=resource):
        adapter._subscribe_local(manager, peer, ['openfmb.essmodule.ESSReadingProfile.m1'])
        adapter._subscribe_local(manager, peer, ['openfmb.essmodule.ESSReadingProfile.m1'])      # already relayed
    resource.subscribe.assert_not_called()                                                     # not the device publication
    adapter.vip.pubsub.subscribe.assert_called_once()
    sub = adapter.vip.pubsub.subscribe.call_args.kwargs
    assert sub['prefix'] == 'openfmb/essmodule/ESSReadingProfile/m1'
    relay = sub['callback']
    relay('pubsub', 'platform.resource_bridge', 'bus', 'openfmb/essmodule/ESSReadingProfile/m1',
          openfmb_bus.headers('essmodule.ESSReadingProfile', 'platform.resource_bridge'), {'essReading': {}})
    sent = json.loads(manager.send.call_args.kwargs['message'].payload)
    assert sent == {'topic': 'openfmb.essmodule.ESSReadingProfile.m1', 'payload': '0a020801', 'encoding': 'hex'}
    manager.send.reset_mock()
    relay('pubsub', 'platform.bus_adapter', 'bus', 'openfmb/essmodule/ESSReadingProfile/m1',
          openfmb_bus.headers('essmodule.ESSReadingProfile', 'platform.bus_adapter'), {'essReading': {}})   # our own inbound republish
    relay('pubsub', 'x', 'bus', 'openfmb/essmodule/ESSReadingProfile/m1/deeper', {}, {})                   # a longer topic under the prefix
    manager.send.assert_not_called()


def test_update_reconfiguration_refreshes_cached_resources(adapter):
    """A configuration update re-resolves every cached resource and drops the ones that no longer resolve."""
    from unittest import mock
    keep, gone = mock.MagicMock(), mock.MagicMock()
    keep.reload.return_value, gone.reload.return_value = True, False
    adapter.resources = {('site/a', False): keep, ('site/b', False): gone}
    adapter.configure_main(None, 'UPDATE', {'bus_type': adapter.config.bus_type or 'mqtt', 'adapters': []})
    keep.reload.assert_called_once(); gone.reload.assert_called_once()
    assert list(adapter.resources) == [('site/a', False)]


ALIAS_DEF = {'kind': 'alias', 'uai': ['site', 'pv', 'remote'], 'data_format': 'fake_pv', 'target_format': 'remote_fmt',
             'publication_topic': 'devices/site/pv/all', 'transform': [{'#': 'transform(device_values())', 'kW': 'transform[W](scale(0.001))'}]}


def test_a_locally_defined_topic_is_built_without_the_service(adapter):
    """A remote topic with a definition in the configuration never reaches the resolve RPC and transforms as declared."""
    adapter.config = MessageBusAdapterConfig(bus_type='mqtt', adapters=[{'name': 'r1', 'host': 'h', 'port': 1}],
                                             definitions={'site/pv/remote': ALIAS_DEF})
    resource = adapter._get_resource_data('site/pv/remote')
    assert resource.origin == 'local' and resource.transform.execute([{'W': 2500}, {}]) == {'kW': 2.5}
    adapter.vip.rpc.call.assert_not_called()
    assert adapter.local_definitions() == {'site/pv/remote': MessageBusAdapterConfig(bus_type='mqtt', definitions={'site/pv/remote': ALIAS_DEF}).definitions['site/pv/remote'].model_dump(mode='json')}


def test_reconfiguration_re_reads_or_removes_local_definitions(adapter):
    """An update re-reads a changed definition in place and drops a resource whose definition went away."""
    adapter.config = MessageBusAdapterConfig(bus_type='mqtt', adapters=[], definitions={'site/pv/remote': ALIAS_DEF, 'site/gone': ALIAS_DEF})
    kept, gone = adapter._get_resource_data('site/pv/remote'), adapter._get_resource_data('site/gone')
    changed = {**ALIAS_DEF, 'transform': [{'#': 'transform(device_values())', 'kW': 'transform[W](scale(0.01))'}]}
    adapter.configure_main(None, 'UPDATE', {'bus_type': 'mqtt', 'adapters': [], 'definitions': {'site/pv/remote': changed}})
    assert adapter.resources[('site/pv/remote', False)] is kept and kept.transform.execute([{'W': 2500}, {}]) == {'kW': 25.0}
    assert ('site/gone', False) not in adapter.resources

