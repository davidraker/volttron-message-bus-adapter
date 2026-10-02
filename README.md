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
  `platform.presentation`; install it with its `openfmb` extra (the `protobuf` runtime) in the
  adapter's environment when any alias declares `"encoding": "protobuf"`

## Configuration

```json
{
    "bus_type": "mqtt",
    "proxy_registration_timeout": 30,
    "adapters": [
        {"name": "site_broker", "host": "broker.example.org", "port": 1883,
         "local_subscriptions": ["openfmb/solarmodule/SolarReadingProfile/7d1a2b3c-0000-4000-8000-000000000001"],
         "remote_subscriptions": ["openfmb/solarmodule/SolarControlProfile/7d1a2b3c-0000-4000-8000-000000000001"]},
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
- `local_subscriptions`: remote topics to serve from local data. Each topic is resolved
  through `platform.presentation` (split on the bus's delimiter, `/` for MQTT and `.` for
  NATS, into a UAI) to a canonical resource; its VOLTTRON publications are transformed into
  the alias's format, encoded as the alias declares, and published on that remote topic for
  as long as the adapter runs, whether or not anyone is subscribed. This is how telemetry
  leaves the platform: declare the topics here, and the remote side subscribes to them in
  the normal way (neither MQTT nor NATS tells a publisher who is listening). These entries
  are the adapter's own and are not passed to the proxy.
- `remote_subscriptions`: topics the proxy subscribes to on the foreign bus as soon as it is
  up. Messages arriving on them are relayed into VOLTTRON as described below. Equivalent to
  calling the `subscribe` RPC at startup.
- `proxy_registration_timeout`: seconds to wait for a newly launched proxy to register.

A proxy is launched for every configured adapter when the configuration is loaded, and its
subscriptions are applied once it has registered. Reloading the configuration re-applies
them; topics already served are skipped, and removing a topic does not stop it until the
agent restarts. Changing `bus_type` at runtime is not supported; restart the agent instead.

## Payload encoding

Payloads cross the adapter-to-proxy link in a JSON envelope, `{"topic": ..., "payload": ...}`.
JSON-serializable payloads travel as they are and are published as UTF-8 JSON text. When the
alias for a topic declares `"encoding": "protobuf"`, `interoperability.resource.ResourceData`
encodes the transformed message with the format's protobuf class and hands the adapter
`bytes`; the adapter hex-encodes them and adds `"encoding": "hex"`, and the proxy publishes
the raw bytes. Inbound binary payloads take the same route in reverse: the proxy forwards
them hex-encoded, the adapter decodes non-UTF-8 payloads to `bytes`, and the resource's codec
turns them back into the dict the transform reads.

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
