import argparse
import asyncio
from uuid import UUID

from app.config import get_settings
from app.db.session import get_sessionmaker
from app.main import configure_logging
from app.worker.runner import WorkerRunner


async def _run(args: argparse.Namespace) -> None:
    configure_logging()
    s = get_settings()
    runner = WorkerRunner(
        sessionmaker=get_sessionmaker(),
        organization_id=UUID(args.org),
        name=args.name,
        concurrency=args.concurrency,
        batch_size=s.claim_batch_size,
        poll_interval_ms=s.poll_interval_ms,
        grace_seconds=s.shutdown_grace_seconds,
    )
    await runner.run()


def main() -> None:
    p = argparse.ArgumentParser(prog="codity-worker")
    # A worker talks to Postgres directly, so it must be told which tenant it serves.
    p.add_argument("--org", required=True, help="organization id this worker serves")
    p.add_argument("--name", default=None, help="worker name (default: hostname-pid)")
    p.add_argument("--concurrency", type=int, default=get_settings().worker_concurrency)
    asyncio.run(_run(p.parse_args()))


if __name__ == "__main__":
    main()
