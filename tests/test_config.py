import pytest
from pydantic import ValidationError

from bus_adapter.config import MessageBusAdapterConfig, MessageBusConfig, MQTTConfig, NATSConfig


def test_default_config_is_idle():
    config = MessageBusAdapterConfig()
    assert config.bus_type == ''
    assert config.adapters == []


def test_missing_keys_do_not_raise_key_error():
    config = MessageBusAdapterConfig(bus_type='mqtt')
    assert config.adapters == []


def test_mqtt_adapters_are_built_and_serialized_fully():
    config = MessageBusAdapterConfig(bus_type='MQTT', adapters=[{'host': 'h', 'port': 1}])
    assert config.bus_type == 'mqtt'
    assert isinstance(config.adapters[0], MQTTConfig)
    dumped = config.model_dump()
    assert dumped['adapters'][0]['host'] == 'h'
    assert dumped['adapters'][0]['port'] == 1


def test_nats_adapter():
    config = MessageBusAdapterConfig(bus_type='nats', adapters=[{'servers': 'nats://x:4222'}])
    assert isinstance(config.adapters[0], NATSConfig)
    assert config.remote_ids() == [('nats', 'nats://x:4222')]


def test_unknown_bus_type_passes_extra_fields_through():
    config = MessageBusAdapterConfig(bus_type='foo', adapters=[{'x': 1, 'y': 'z'}])
    adapter = config.adapters[0]
    assert type(adapter) is MessageBusConfig
    assert adapter.proxy_kwargs() == {'x': 1, 'y': 'z'}
    assert adapter.unique_remote_id('foo') == ('foo', 1, 'z')


def test_invalid_adapter_raises_validation_error():
    with pytest.raises(ValidationError):
        MessageBusAdapterConfig(bus_type='mqtt', adapters=[{'port': 1}])    # host missing
    with pytest.raises(ValidationError):
        MessageBusAdapterConfig(bus_type='mqtt', adapters='not-a-list')
    with pytest.raises(ValidationError):
        MessageBusAdapterConfig(bus_type='mqtt', adapters=[5])


def test_unique_remote_id_and_lookup():
    config = MessageBusAdapterConfig(bus_type='mqtt', adapters=[
        {'name': 'named', 'host': 'a'}, {'host': 'b', 'port': 2}])
    assert config.remote_ids() == [('mqtt', 'named'), ('mqtt', 'b', 2)]
    assert config.find_adapter(['mqtt', 'named']).host == 'a'
    assert config.find_adapter(('mqtt', 'b', 2)).host == 'b'
    assert config.find_adapter(('mqtt', 'nope')) is None
    assert config.adapters[0].proxy_kwargs() == {'host': 'a', 'port': 1883, 'keepalive': 60,
                                                 'bind_address': '', 'bind_port': 0}


def test_existing_instances_are_accepted():
    config = MessageBusAdapterConfig(bus_type='mqtt', adapters=[MQTTConfig(host='h')])
    assert config.adapters[0].host == 'h'
    copy = config.model_copy()
    assert copy.adapters[0].host == 'h'


def test_timeout_must_be_positive():
    with pytest.raises(ValidationError):
        MessageBusAdapterConfig(proxy_registration_timeout=0)
