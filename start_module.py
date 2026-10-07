#!/usr/bin/env python3
"""
kafka-monitor: READ-ONLY Kafka consumer diagnostics for kafka-bulk-consumer modules.

What it does per configured topic (all results go to the module log as "DIAG ..." lines,
summary in the message, full JSON in the description):
  1. topic_meta      partitions, leaders, replicas, ISR, topic configs
  2. group_state     the production consumer group: state, members, hosts, assignments
  3. group_lag       per-partition low/high watermark, committed offset, lag
  4. replay          consume(num_messages=batchSize, timeout=pollTimeout) from the group's
                     committed offsets with a THROWAWAY group: msgs/bytes per poll,
                     per-partition spread, empty polls. Run once per fetch profile.
  5. msg_size        message size distribution from the replay
  6. client_stats    librdkafka statistics: broker throttle, rtt, fetch queue
  7. subscribe_test  how many empty polls a fresh subscribe() gets before first message

Safety:
  - Never joins the production consumer group, never commits offsets
    (enable.auto.commit=false, enable.auto.offset.store=false, no commit calls).
  - Never produces messages.
  - Never logs settings values for credentials.
"""

import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
import traceback
import uuid
from pathlib import Path

try:
    from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, KafkaException, TopicPartition
    from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType
    from onevizion import LogLevel, ModuleLog
except ImportError:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])
    os.execv(sys.executable, [sys.executable] + sys.argv)

MODULE_VERSION = '1.1'
SECRET_WORDS = ('secret', 'password', 'key', 'token', 'cert', 'ca')

_cert_files = []


# ---------------------------------------------------------------- logging

class Diag:
    def __init__(self, module_log):
        self._log = module_log

    def info(self, section, summary, data=None):
        msg = f'DIAG {section}: {summary}'
        desc = json.dumps(data, default=str, indent=1) if data is not None else None
        print(msg)
        if desc:
            print(desc)
        self._log.add(LogLevel.INFO, msg[:2000], desc)

    def error(self, section, summary, exc=None):
        msg = f'DIAG {section} ERROR: {summary}'
        desc = traceback.format_exc() if exc else None
        print(msg, file=sys.stderr)
        self._log.add(LogLevel.ERROR, msg[:2000], desc)


# ---------------------------------------------------------------- kafka config

def write_cert(content, name):
    f = tempfile.NamedTemporaryFile('w', delete=False, suffix=f'_{name}.pem')
    f.write(content)
    f.close()
    _cert_files.append(Path(f.name))
    return f.name


def kafka_auth_config(s):
    """Same auth options and setting names as kafka-bulk-consumer."""
    cfg = {'bootstrap.servers': s['kafkaBootstrapServers']}
    auth = s.get('kafkaAuthType', 'PLAINTEXT')
    if auth == 'PLAINTEXT':
        cfg['security.protocol'] = 'PLAINTEXT'
    elif auth == 'SASL':
        cfg.update({
            'security.protocol': s.get('kafkaSecurityProtocol') or 'SASL_PLAINTEXT',
            'sasl.mechanism': s.get('kafkaSaslMechanism') or 'PLAIN',
            'sasl.username': s['kafkaSaslUsername'],
            'sasl.password': s['kafkaSaslPassword'],
        })
    elif auth == 'SASL_OAUTH2':
        cfg.update({
            'security.protocol': s.get('kafkaSecurityProtocol') or 'SASL_SSL',
            'sasl.mechanism': s.get('kafkaSaslMechanism') or 'OAUTHBEARER',
            'sasl.oauthbearer.method': s.get('kafkaSaslOauthbearerMethod') or 'oidc',
            'sasl.oauthbearer.token.endpoint.url': s['kafkaSaslOauthbearerTokenEndpointUrl'],
            'sasl.oauthbearer.client.id': s['kafkaSaslOauthbearerClientId'],
            'sasl.oauthbearer.client.secret': s['kafkaSaslOauthbearerClientSecret'],
        })
        if s.get('kafkaSaslOauthbearerScope'):
            cfg['sasl.oauthbearer.scope'] = s['kafkaSaslOauthbearerScope']
    elif auth == 'M_TLS':
        cfg.update({
            'security.protocol': s.get('kafkaSecurityProtocol') or 'SSL',
            'ssl.ca.location': write_cert(s['kafkaSslCa'], 'ca'),
            'ssl.certificate.location': write_cert(s['kafkaSslCert'], 'cert'),
            'ssl.key.location': write_cert(s['kafkaSslKey'], 'key'),
        })
        if s.get('kafkaSslKeyPassword'):
            cfg['ssl.key.password'] = s['kafkaSslKeyPassword']
    else:
        raise ValueError(f'Unknown kafkaAuthType: {auth}')

    if cfg.get('security.protocol') in ('SASL_SSL', 'SSL'):
        cfg['ssl.endpoint.identification.algorithm'] = 'https'
        if auth != 'M_TLS' and (s.get('kafkaSslCa') or '').strip():
            cfg['ssl.ca.location'] = write_cert(s['kafkaSslCa'], 'ca')
    return cfg


def safe_cfg(cfg):
    """Config with credential values masked, for logging."""
    return {k: ('***' if any(w in k for w in SECRET_WORDS) and 'location' not in k else v)
            for k, v in cfg.items()}


def wait(futures, timeout):
    return {k: f.result(timeout=timeout) for k, f in futures.items()}


# ---------------------------------------------------------------- checks

def topic_meta(diag, admin, topic, timeout):
    md = admin.list_topics(topic=topic, timeout=timeout)
    t = md.topics.get(topic)
    if t is None or t.error is not None:
        raise RuntimeError(f'topic {topic} not found or error: {t.error if t else "missing"}')
    parts = []
    for pid, p in sorted(t.partitions.items()):
        parts.append({'partition': pid, 'leader': p.leader, 'replicas': p.replicas, 'isr': p.isrs,
                      'under_replicated': len(p.isrs) < len(p.replicas),
                      'error': str(p.error) if p.error else None})
    brokers = {b.id: f'{b.host}:{b.port}' for b in md.brokers.values()}

    configs = {}
    try:
        res = ConfigResource(ResourceType.TOPIC, topic)
        cfg = list(admin.describe_configs([res]).values())[0].result(timeout=timeout)
        wanted = ('max.message.bytes', 'compression.type', 'retention.ms', 'retention.bytes',
                  'segment.bytes', 'cleanup.policy', 'message.timestamp.type')
        configs = {k: v.value for k, v in cfg.items() if k in wanted}
    except Exception as e:
        configs = {'error': f'describe_configs not allowed or failed: {e}'}

    leaders = {}
    for p in parts:
        leaders[p['leader']] = leaders.get(p['leader'], 0) + 1
    diag.info('topic_meta', f'{topic}: {len(parts)} partitions, leaders per broker {leaders}, '
                            f'under-replicated {sum(p["under_replicated"] for p in parts)}',
              {'topic': topic, 'partitions': parts, 'brokers': brokers, 'configs': configs})
    return [p['partition'] for p in parts]


def group_state(diag, admin, topic, group, timeout):
    try:
        d = wait(admin.describe_consumer_groups([group]), timeout)[group]
    except Exception as e:
        diag.error('group_state', f'{group}: describe_consumer_groups failed (missing DESCRIBE on group?): {e}')
        return
    members = []
    for m in d.members:
        tps = m.assignment.topic_partitions if m.assignment else []
        members.append({'client_id': m.client_id, 'host': m.host, 'member_id': m.member_id,
                        'assigned': [f'{tp.topic}[{tp.partition}]' for tp in tps]})
    other_topics = sorted({a.split('[')[0] for m in members for a in m['assigned']} - {topic})
    diag.info('group_state', f'{group}: state {d.state}, {len(members)} member(s) '
                             f'on hosts {sorted({m["host"] for m in members})}'
                             + (f', ALSO consuming {other_topics}' if other_topics else ''),
              {'group': group, 'state': str(d.state), 'coordinator': str(d.coordinator),
               'partition_assignor': d.partition_assignor, 'members': members})


def group_lag(diag, admin, consumer, topic, group, partitions, timeout):
    committed = {}
    try:
        req = ConsumerGroupTopicPartitions(group, [TopicPartition(topic, p) for p in partitions])
        res = wait(admin.list_consumer_group_offsets([req]), timeout)[group]
        committed = {tp.partition: tp.offset for tp in res.topic_partitions}
    except Exception as e:
        diag.error('group_lag', f'{group}: list_consumer_group_offsets failed: {e}')

    rows, total = [], 0
    for p in partitions:
        t0 = time.monotonic()
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout)
        took = round(time.monotonic() - t0, 2)
        c = committed.get(p, -1001)
        lag = high - c if c >= 0 else None
        total += lag or 0
        rows.append({'partition': p, 'low': low, 'high': high, 'committed': c, 'lag': lag,
                     'watermark_query_sec': took})
    diag.info('group_lag', f'{group} on {topic}: total lag {total}, per partition '
                           f'{[r["lag"] for r in rows]}', {'group': group, 'partitions': rows})
    return {r['partition']: (r['committed'] if r['committed'] >= 0 else r['low']) for r in rows}


def replay(diag, base, topic, start_offsets, profile_name, overrides, rp):
    stats_box = {}

    def on_stats(js):
        stats_box['last'] = js

    cfg = dict(base)
    cfg.update({
        'group.id': f'kafka-monitor-diag-{uuid.uuid4().hex[:12]}',
        'enable.auto.commit': False,
        'enable.auto.offset.store': False,
        'auto.offset.reset': 'earliest',
        'statistics.interval.ms': 5000,
        'stats_cb': on_stats,
    })
    cfg.update(overrides or {})

    consumer = Consumer(cfg)
    consumer.assign([TopicPartition(topic, p, o) for p, o in start_offsets.items()])

    polls, sizes, per_part = [], [], {}
    started = time.monotonic()
    try:
        for i in range(rp['polls']):
            if time.monotonic() - started > rp['maxSeconds']:
                break
            t0 = time.monotonic()
            msgs = consumer.consume(num_messages=rp['batchSize'], timeout=rp['pollTimeout'])
            took = time.monotonic() - t0
            n_ok, n_err, nbytes, parts = 0, 0, 0, {}
            for m in msgs:
                if m.error():
                    n_err += 1
                    continue
                n_ok += 1
                size = len(m.value() or b'') + len(m.key() or b'') + sum(
                    len(k) + len(v or b'') for k, v in (m.headers() or []))
                sizes.append(size)
                nbytes += size
                parts[m.partition()] = parts.get(m.partition(), 0) + 1
            for p, c in parts.items():
                per_part[p] = per_part.get(p, 0) + c
            polls.append({'i': i, 'msgs': n_ok, 'errors': n_err, 'bytes': nbytes,
                          'sec': round(took, 3), 'partitions': parts})
        time.sleep(0.1)
        consumer.poll(0)  # let a final stats callback fire
    finally:
        consumer.close()  # no commit: enable.auto.commit=false and nothing stored

    counts = [p['msgs'] for p in polls]
    non_empty = [c for c in counts if c]
    elapsed = time.monotonic() - started
    summary = {
        'profile': profile_name, 'overrides': safe_cfg(overrides or {}),
        'polls': len(polls), 'empty_polls': counts.count(0),
        'msgs_total': sum(counts), 'avg_per_poll': round(statistics.mean(counts), 1) if counts else 0,
        'avg_per_non_empty_poll': round(statistics.mean(non_empty), 1) if non_empty else 0,
        'full_batches': sum(1 for c in counts if c >= rp['batchSize']),
        'msgs_per_sec': round(sum(counts) / elapsed, 1) if elapsed else 0,
        'mb_per_sec': round(sum(p['bytes'] for p in polls) / elapsed / 1048576, 3) if elapsed else 0,
        'per_partition_total': per_part,
        'poll_detail': polls,
    }
    diag.info('replay', f'{topic} [{profile_name}]: {summary["polls"]} polls, '
                        f'avg {summary["avg_per_poll"]} msgs/poll, {summary["full_batches"]} full batches, '
                        f'{summary["empty_polls"]} empty, {summary["msgs_per_sec"]} msg/s, '
                        f'{summary["mb_per_sec"]} MB/s, partitions {per_part}', summary)

    if sizes:
        s = sorted(sizes)
        pct = lambda q: s[min(len(s) - 1, int(len(s) * q))]
        diag.info('msg_size', f'{topic} [{profile_name}]: n={len(s)} avg {int(statistics.mean(s))} B, '
                              f'p50 {pct(.5)} B, p95 {pct(.95)} B, max {s[-1]} B',
                  {'n': len(s), 'min': s[0], 'avg': statistics.mean(s), 'p50': pct(.5),
                   'p95': pct(.95), 'p99': pct(.99), 'max': s[-1]})

    if 'last' in stats_box:
        st = json.loads(stats_box['last'])
        brokers = {b.get('name'): {'state': b.get('state'),
                                   'throttle_avg_ms': (b.get('throttle') or {}).get('avg'),
                                   'throttle_max_ms': (b.get('throttle') or {}).get('max'),
                                   'rtt_avg_us': (b.get('rtt') or {}).get('avg'),
                                   'rtt_p99_us': (b.get('rtt') or {}).get('p99'),
                                   'fetch_wakeups': b.get('wakeups')}
                   for b in st.get('brokers', {}).values() if b.get('nodeid', -1) >= 0}
        tparts = (st.get('topics', {}).get(topic) or {}).get('partitions', {})
        fetch = {pid: {'fetch_state': p.get('fetch_state'), 'fetchq_cnt': p.get('fetchq_cnt'),
                       'fetchq_size': p.get('fetchq_size'), 'leader': p.get('leader')}
                 for pid, p in tparts.items() if pid != '-1'}
        # Throttle across EVERY stats window (5 s each), not only the last snapshot
        series = {}
        for raw in stats_box['all']:
            snap = json.loads(raw)
            for b in snap.get('brokers', {}).values():
                if b.get('nodeid', -1) < 0:
                    continue
                series.setdefault(b.get('nodeid'), []).append((b.get('throttle') or {}).get('max') or 0)
        windows = len(stats_box['all'])
        per_broker = {nid: {'windows': len(v), 'throttled_windows': sum(1 for x in v if x > 0),
                            'max_ms': max(v), 'avg_ms': round(statistics.mean(v), 1)}
                      for nid, v in sorted(series.items())}
        max_thr = max([v['max_ms'] for v in per_broker.values()] or [0])
        thr_windows = max([v['throttled_windows'] for v in per_broker.values()] or [0])
        diag.info('client_stats', f'{topic} [{profile_name}]: max broker throttle {max_thr} ms, '
                                  f'throttled in up to {thr_windows}/{windows} stats windows, '
                                  f'per broker {{id: max_ms}} '
                                  f'{ {k: v["max_ms"] for k, v in per_broker.items()} }',
                  {'throttle_per_broker': per_broker, 'last_snapshot_brokers': brokers,
                   'partitions': fetch})
    return summary


def subscribe_test(diag, base, topic, rp):
    cfg = dict(base)
    cfg.update({'group.id': f'kafka-monitor-sub-{uuid.uuid4().hex[:12]}',
                'enable.auto.commit': False, 'enable.auto.offset.store': False,
                'auto.offset.reset': 'earliest'})
    consumer = Consumer(cfg)
    events = []
    consumer.subscribe([topic], on_assign=lambda c, ps: events.append(
        ('assigned', round(time.monotonic() - t0, 2), [p.partition for p in ps])))
    t0 = time.monotonic()
    empty_before_first, first_at = 0, None
    try:
        for _ in range(20):
            msgs = consumer.consume(num_messages=rp['batchSize'], timeout=rp['pollTimeout'])
            if [m for m in msgs if not m.error()]:
                first_at = round(time.monotonic() - t0, 2)
                break
            empty_before_first += 1
    finally:
        consumer.close()
    diag.info('subscribe_test', f'{topic}: fresh subscribe got {empty_before_first} empty poll(s) '
                                f'before first message (first at {first_at}s); module exits after 3',
              {'empty_polls_before_first': empty_before_first, 'first_message_sec': first_at,
               'assign_events': events, 'poll_timeout_sec': rp['pollTimeout']})


# ---------------------------------------------------------------- main

def main():
    with open('settings.json') as f:
        settings = json.load(f)
    params_file = Path('ihub_parameters.json')
    if params_file.exists():
        with open(params_file) as f:
            run = json.load(f)
        module_log = ModuleLog(run['processId'], run['ovUrl'], settings['ovAccessKey'],
                               settings['ovSecretKey'], None, True, run.get('logLevel', 'info'))
    else:
        class _Print:
            def add(self, level, message, description=None):
                pass
        module_log = _Print()

    diag = Diag(module_log)
    rp = {'batchSize': 1000, 'pollTimeout': 5.0, 'polls': 30, 'maxSeconds': 240}
    rp.update(settings.get('replay') or {})
    timeout = float(settings.get('adminTimeoutSec', 30))

    base = kafka_auth_config(settings)
    diag.info('start', f'kafka-monitor {MODULE_VERSION}, {len(settings["targets"])} target(s)',
              {'kafka_config': safe_cfg(base), 'replay': rp,
               'profiles': {k: safe_cfg(v) for k, v in (settings.get('fetchProfiles') or {}).items()}})

    admin = AdminClient(dict(base))
    probe = Consumer(dict(base, **{'group.id': f'kafka-monitor-probe-{uuid.uuid4().hex[:12]}',
                                   'enable.auto.commit': False}))
    failures = 0
    try:
        for target in settings['targets']:
            topic, group = target['topic'], target['groupId']
            name = target.get('name', topic)
            diag.info('target', f'=== {name}: topic {topic}, group {group} ===')
            try:
                partitions = topic_meta(diag, admin, topic, timeout)
                group_state(diag, admin, topic, group, timeout)
                start = group_lag(diag, admin, probe, topic, group, partitions, timeout)
                profiles = {'module_default': {}}
                profiles.update(settings.get('fetchProfiles') or {})
                for pname, overrides in profiles.items():
                    replay(diag, base, topic, start, pname, overrides, rp)
                if settings.get('runSubscribeTest', True):
                    subscribe_test(diag, base, topic, rp)
            except Exception as e:
                failures += 1
                diag.error('target', f'{name}: {e}', e)
    finally:
        probe.close()
        for p in _cert_files:
            try:
                p.unlink()
            except Exception:
                pass

    diag.info('done', f'finished, {failures} target(s) failed')
    if failures:
        sys.exit(1)


if __name__ == '__main__':
    main()
