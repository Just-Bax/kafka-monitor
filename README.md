# kafka-monitor

Read-only Kafka diagnostics for kafka-bulk-consumer modules. Run it manually once; read the
`DIAG ...` lines in the module log (summary in the message, JSON in the description).

## Safety
- Uses throwaway consumer groups (`kafka-monitor-*`), never the production group.
- Never commits offsets (`enable.auto.commit=false`, `enable.auto.offset.store=false`).
- Never produces messages. Credentials are masked in logs.

## Checks per target
| section | answers |
|---|---|
| topic_meta | partitions, leaders, ISR, topic configs (max.message.bytes, compression) |
| group_state | production group state, members, hosts, assignments (extra consumers? other topics in the group?) |
| group_lag | per-partition watermarks, committed offset, lag, broker query time |
| replay | msgs/bytes per `consume(batchSize, pollTimeout)` from the group's committed offsets, per fetch profile |
| msg_size | message size distribution |
| client_stats | broker throttle time, rtt, fetch queue state |
| subscribe_test | empty polls before first message on a fresh subscribe (module exits after 3) |

## Settings
Auth settings have the same names as kafka-bulk-consumer. `targets` lists topic + production
`groupId` pairs to compare. `fetchProfiles` are extra consumer config overrides to replay with,
in addition to `module_default` (no overrides, same as the bulk consumer).
