"""
Concord CLI.

    concord demo     run a self-contained demo against in-memory infra
    concord bench    run the load simulation and print latency percentiles
    concord relay    run the real relay against configured infra (needs [prod])

`demo` and `bench` need no external services, which is deliberate: a reviewer
can `pip install -e . && concord demo` and watch the guarantees hold in ten
seconds without standing up Postgres.
"""

from __future__ import annotations

import argparse
import json
import sys

from . import __version__
from .config import Config
from .logging_ import configure, get_logger

log = get_logger("concord.cli")


def cmd_demo(args: argparse.Namespace) -> int:
    from .memory import FlakyBroker, MemoryArchive, MemoryDedupe, MemoryStore
    from .model import OutboxRecord
    from .relay import Relay

    configure(args.log_level)
    store = MemoryStore()
    broker = FlakyBroker(fail_rate=args.fail_rate, seed=7)
    cfg = Config(max_attempts=6, backoff_base_s=0.01, backoff_cap_s=0.2,
                 log_level=args.log_level)
    relay = Relay(store, broker, MemoryArchive(), cfg, dedupe=MemoryDedupe())

    for i in range(args.count):
        store.enqueue(OutboxRecord(aggregate="order",
                                   payload=json.dumps({"order_id": i}).encode(),
                                   key=f"cust-{i % 8}"))

    # Drive the relay until everything is terminal.
    import time
    deadline = time.time() + 10
    while store.snapshot()["PENDING"] > 0 and time.time() < deadline:
        relay.tick()
        time.sleep(0.005)

    snap = store.snapshot()
    pct = relay.metrics.percentiles()
    print(json.dumps({
        "event": "demo.summary",
        "enqueued": args.count,
        "published": snap["PUBLISHED"],
        "dead_lettered": snap["DEAD"],
        "retries": relay.metrics.retried,
        "duplicates_dropped": relay.metrics.duplicates_dropped,
        "publish_latency_ms": pct,
    }, indent=2))
    # Zero PENDING left means no record was lost: every one is PUBLISHED or DEAD.
    return 0 if snap["PENDING"] == 0 else 1


def cmd_bench(args: argparse.Namespace) -> int:
    from benchmark.harness import main as bench_main
    bench_main()
    return 0


def cmd_relay(args: argparse.Namespace) -> int:
    cfg = Config.from_env()
    configure(cfg.log_level)
    try:
        from .adapters.kafka import KafkaBroker
        from .adapters.minio_ import MinioArchive
        from .adapters.postgres import PostgresStore
        from .adapters.redis_ import RedisDedupe
    except RuntimeError as e:
        log.error("missing_prod_deps", extra={"hint": str(e)})
        return 2
    import os
    store = PostgresStore(cfg.database_url)
    store.init_schema()
    broker = KafkaBroker(cfg.kafka_bootstrap)
    dedupe = RedisDedupe(cfg.redis_url)
    archive = MinioArchive(cfg.minio_endpoint, cfg.minio_bucket,
                           os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
                           os.environ.get("MINIO_SECRET_KEY", "minioadmin"))
    from .relay import Relay
    relay = Relay(store, broker, archive, cfg, dedupe=dedupe)
    log.info("relay.start", extra={"batch_size": cfg.batch_size})
    relay.run_forever()
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="concord",
                                description="Transactional outbox relay.")
    p.add_argument("--version", action="version", version=f"concord {__version__}")
    sub = p.add_subparsers(dest="command", required=True)

    d = sub.add_parser("demo", help="run against in-memory infra")
    d.add_argument("--count", type=int, default=500)
    d.add_argument("--fail-rate", type=float, default=0.15)
    d.add_argument("--log-level", default="INFO")
    d.set_defaults(func=cmd_demo)

    b = sub.add_parser("bench", help="run the load simulation")
    b.set_defaults(func=cmd_bench)

    r = sub.add_parser("relay", help="run the real relay (needs concord[prod])")
    r.set_defaults(func=cmd_relay)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
