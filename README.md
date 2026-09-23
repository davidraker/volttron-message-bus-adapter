# VOLTTRON Message Bus Adapter

The Message Bus Adapter agent relays data between VOLTTRON and a foreign message bus
(currently MQTT or NATS). Each remote bus connection runs in its own protocol proxy
subprocess, managed through the `protocol-proxy` library. Topics arriving from a remote
bus are resolved to canonical resources through the `platform.presentation` service and
transformed before being republished on VOLTTRON, and vice versa.

## Requirements

- `volttron-core` >= 2.0.0rc30
- `protocol-proxy` >= 2.0.0rc1 plus the plugin for the bus type
  (`protocol_proxy.protocol.mqtt` or `protocol_proxy.protocol.nats`)
- `interoperability-service` (provides `interoperability.resource`) running as
  `platform.presentation`

## Configuration

```json
{
    "bus_type": "mqtt",
    "proxy_registration_timeout": 30,
    "adapters": [
        {"name": "site_broker", "host": "broker.example.org", "port": 1883},
        {"host": "10.0.0.5", "port": 8883, "keepalive": 30}
    ]
}
```

- `bus_type`: protocol plugin name. Resolved to `protocol_proxy.protocol.<bus_type>`.
- `adapters`: one entry per remote bus. All fields except `name` are passed to the proxy
  process as command line arguments, so they must match the proxy's parameters.
  - MQTT: `host`, `port`, `keepalive`, `bind_address`, `bind_port`, `client_id`, `username`,
    `password`, `tls`, `protocol` (`MQTTv31`/`MQTTv311`/`MQTTv5`), `qos`, `reconnect_min_delay`,
    `reconnect_max_delay`.
  - NATS: `servers` (URL or list of URLs), `name`, `user`, `password`, `nats_token`,
    `connect_timeout`, `max_reconnect_attempts`, `reconnect_time_wait`, `tls`.
  Optional fields left unset are not sent, so the proxy's own defaults apply.
- `name`: optional handle. The remote's `unique_remote_id` is `[bus_type, name]` when
  set, otherwise `[bus_type, host, port]` for MQTT and `[bus_type, servers]` for NATS.
- `proxy_registration_timeout`: seconds to wait for a newly launched proxy to register.

A proxy is launched for every configured adapter when the configuration is loaded.
Changing `bus_type` at runtime is not supported; restart the agent instead.

## RPC interface

| Method | Arguments | Description |
| --- | --- | --- |
| `list_remotes()` | | Configured `unique_remote_id`s. |
| `subscribe(unique_remote_id, topics)` | id, str or list of str | Subscribe the remote proxy to topics on the foreign bus. |
| `publish(unique_remote_id, topic, payload)` | id, str, JSON-serializable | Publish to the foreign bus. |

Only remotes present in `adapters` can be addressed.

## Development

```bash
pip install -e .
pytest
```
