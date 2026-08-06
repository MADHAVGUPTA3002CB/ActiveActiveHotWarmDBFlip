from __future__ import annotations

import argparse
import json
import uuid

from .bootstrap import bootstrap
from .core import FenceWakeupMode, build_manifest, canonical_manifest_json
from .flip import FlipRunner
from .postgres_io import guarded_insert_events
from .scenario import prepare_paused_backlog, prepare_production_workload, prepare_running_overload
from .settings import Settings


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(prog="flipbench")
    subcommands = command.add_subparsers(dest="command", required=True)

    setup = subcommands.add_parser("setup", help="create tables/topics and register connectors")
    setup.add_argument("--tables", type=int, choices=(5, 10, 15, 20), default=None)

    rolling = subcommands.add_parser(
        "rolling",
        help="H-DD-Prod: run consecutive generation-pinned provision/rotate/flip cycles",
    )
    rolling.add_argument("--tables", type=int, choices=(5, 10, 15, 20), default=None)
    rolling.add_argument("--generations", type=int, default=3)
    rolling.add_argument("--active-tps", type=int, default=300)
    rolling.add_argument("--retiring-tps", type=int, default=40)
    rolling.add_argument("--duration-seconds", type=float, default=15.0)
    rolling.add_argument("--payload-bytes", type=int, default=256)
    rolling.add_argument("--flip-timeout-seconds", type=float, default=120.0)
    rolling.add_argument("--quiesce-seconds", type=float, default=3.0)

    manifest = subcommands.add_parser("manifest", help="print the canonical route manifest")
    manifest.add_argument("--tables", type=int, choices=(5, 10, 15, 20), default=5)

    load = subcommands.add_parser("load", help="insert tagged events through hot parent tables")
    load.add_argument("--tables", type=int, choices=(5, 10, 15, 20), default=None)
    load.add_argument("--events-per-table", type=int, required=True)
    load.add_argument("--payload-bytes", type=int, default=512)
    load.add_argument("--timeslot", choices=("retiring", "active"), default="retiring")
    load.add_argument("--run-id", type=uuid.UUID, default=None)

    lag = subcommands.add_parser("prepare-lag", help="create deterministic paused source and sink backlog")
    lag.add_argument("--tables", type=int, choices=(5, 10, 15, 20), default=None)
    lag.add_argument("--source-events-per-table", type=int, default=1000)
    lag.add_argument("--sink-events-per-table", type=int, default=2000)
    lag.add_argument("--payload-bytes", type=int, default=512)

    flip = subcommands.add_parser("flip", help="run the fail-closed flip and write run.json")
    flip.add_argument("--tables", type=int, choices=(5, 10, 15, 20), default=None)
    flip.add_argument("--run-id", type=uuid.UUID, required=True)
    flip.add_argument("--timeout-seconds", type=float, default=120.0)
    flip.add_argument("--poll-ms", type=float, default=100.0)
    flip.add_argument("--resume-paused", action="store_true")
    flip.add_argument(
        "--fence-wakeup-mode",
        choices=tuple(mode.value for mode in FenceWakeupMode),
        default=FenceWakeupMode.PASSIVE.value,
    )

    benchmark = subcommands.add_parser("benchmark", help="prepare paused non-zero lag and immediately run one flip")
    benchmark.add_argument("--tables", type=int, choices=(5, 10, 15, 20), default=None)
    benchmark.add_argument("--source-events-per-table", type=int, default=1000)
    benchmark.add_argument("--sink-events-per-table", type=int, default=2000)
    benchmark.add_argument("--payload-bytes", type=int, default=512)
    benchmark.add_argument("--timeout-seconds", type=float, default=120.0)
    benchmark.add_argument("--poll-ms", type=float, default=100.0)
    benchmark.add_argument(
        "--fence-wakeup-mode",
        choices=tuple(mode.value for mode in FenceWakeupMode),
        default=FenceWakeupMode.PASSIVE.value,
    )

    running = subcommands.add_parser(
        "benchmark-running",
        help="create healthy source and sink lag under continuous writes and immediately run one flip",
    )
    running.add_argument("--tables", type=int, choices=(5, 10, 15, 20), default=None)
    running.add_argument("--batch-events-per-table", type=int, default=1000)
    running.add_argument("--payload-bytes", type=int, default=512)
    running.add_argument("--min-source-lag-bytes", type=int, default=65_536)
    running.add_argument("--min-source-lag-records-per-partition", type=int, default=100)
    running.add_argument("--min-sink-lag-records-per-partition", type=int, default=100)
    running.add_argument("--stable-samples", type=int, default=3)
    running.add_argument("--max-admitted-rows-per-partition", type=int, default=50_000)
    running.add_argument("--max-batches", type=int, default=100)
    running.add_argument("--admission-timeout-seconds", type=float, default=30.0)
    running.add_argument("--timeout-seconds", type=float, default=120.0)
    running.add_argument("--poll-ms", type=float, default=50.0)
    running.add_argument(
        "--fence-wakeup-mode",
        choices=tuple(mode.value for mode in FenceWakeupMode),
        default=FenceWakeupMode.PASSIVE.value,
    )

    prodlike = subcommands.add_parser(
        "benchmark-prodlike",
        help="run active-heavy/retiring-light traffic with a bounded retiring-timeslot flip",
    )
    prodlike.add_argument("--tables", type=int, choices=(5, 10, 15, 20), default=None)
    prodlike.add_argument("--active-events-per-table", type=int, default=100)
    prodlike.add_argument("--retiring-events-per-table", type=int, default=1)
    prodlike.add_argument("--active-pause-ms", type=float, default=5.0)
    prodlike.add_argument("--retiring-pause-ms", type=float, default=50.0)
    prodlike.add_argument("--payload-bytes", type=int, default=512)
    prodlike.add_argument("--max-source-lag-bytes", type=int, default=8_388_608)
    prodlike.add_argument("--max-sink-lag-records-per-partition", type=int, default=10)
    prodlike.add_argument("--stable-samples", type=int, default=3)
    prodlike.add_argument("--max-batches", type=int, default=1_000_000)
    prodlike.add_argument("--admission-timeout-seconds", type=float, default=30.0)
    prodlike.add_argument("--park-budget-ms", type=float, default=200.0)
    prodlike.add_argument("--revert-reserve-ms", type=float, default=50.0)
    prodlike.add_argument("--poll-ms", type=float, default=5.0)
    prodlike.add_argument(
        "--fence-wakeup-mode",
        choices=tuple(mode.value for mode in FenceWakeupMode),
        default=FenceWakeupMode.PASSIVE.value,
    )
    return command


def main() -> None:
    args = parser().parse_args()
    if args.command == "manifest":
        print(canonical_manifest_json(build_manifest(args.tables, "cell01", "retiring")))
        return

    settings = Settings.from_env(args.tables)
    manifest = build_manifest(settings.table_count, settings.cell, settings.timeslot)
    if args.command == "setup":
        if settings.source_topology == "lanes":
            from .generations import bootstrap_lanes

            configured = bootstrap_lanes(settings)
        else:
            configured = bootstrap(settings)
        print(canonical_manifest_json(configured))
    elif args.command == "rolling":
        import datetime as _datetime

        from .rolling import run_rolling

        if settings.source_topology != "lanes":
            raise SystemExit("rolling requires SOURCE_TOPOLOGY=lanes (run setup with lanes first)")
        report = run_rolling(
            settings,
            generations=args.generations,
            active_tps=args.active_tps,
            retiring_tps=args.retiring_tps,
            duration_seconds=args.duration_seconds,
            payload_bytes=args.payload_bytes,
            flip_timeout_seconds=args.flip_timeout_seconds,
            quiesce_seconds=args.quiesce_seconds,
        )
        stamp = _datetime.datetime.now(_datetime.timezone.utc).strftime("%Y%m%d-%H%M%S")
        out_dir = settings.results_dir / f"h-dd-prod-rolling-{stamp}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "rolling.json"
        out_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps({"outcome": report["outcome"], "results": str(out_path)}, sort_keys=True))
        if report["outcome"] != "success":
            raise SystemExit(1)
    elif args.command == "load":
        run_id = args.run_id or uuid.uuid4()
        inserted = guarded_insert_events(
            settings.hot_dsn,
            settings.warm_dsn,
            manifest,
            run_id,
            args.events_per_table,
            args.timeslot,
            args.payload_bytes,
        )
        print(json.dumps({"run_id": str(run_id), "inserted": inserted}, sort_keys=True))
    elif args.command == "prepare-lag":
        prepared = prepare_paused_backlog(
            settings,
            manifest,
            args.sink_events_per_table,
            args.source_events_per_table,
            args.payload_bytes,
        )
        print(json.dumps({"run_id": str(prepared.run_id), "sink_events": prepared.sink_events, "source_events": prepared.source_events, "source_lag_bytes": prepared.source_lag_bytes}, sort_keys=True))
    elif args.command == "flip":
        result = FlipRunner(
            settings,
            args.run_id,
            args.timeout_seconds,
            args.poll_ms / 1000,
            fence_wakeup_mode=args.fence_wakeup_mode,
        ).run(args.resume_paused)
        print(json.dumps({"outcome": result["outcome"], "run_id": result["run_id"], "writer_park_ns": result["durations_ns"]["writer_park_ns"]}, sort_keys=True))
    elif args.command == "benchmark":
        prepared = prepare_paused_backlog(
            settings,
            manifest,
            args.sink_events_per_table,
            args.source_events_per_table,
            args.payload_bytes,
        )
        result = FlipRunner(
            settings,
            prepared.run_id,
            args.timeout_seconds,
            args.poll_ms / 1000,
            fence_wakeup_mode=args.fence_wakeup_mode,
        ).run(True)
        print(json.dumps({"outcome": result["outcome"], "run_id": result["run_id"], "writer_park_ns": result["durations_ns"]["writer_park_ns"]}, sort_keys=True))
    elif args.command == "benchmark-running":
        overload = prepare_running_overload(
            settings,
            manifest,
            args.batch_events_per_table,
            args.payload_bytes,
            args.min_source_lag_bytes,
            args.min_source_lag_records_per_partition,
            args.min_sink_lag_records_per_partition,
            args.stable_samples,
            args.max_admitted_rows_per_partition,
            args.max_batches,
            args.admission_timeout_seconds,
            args.poll_ms / 1000,
        )
        flip_error: BaseException | None = None
        result = None
        try:
            result = FlipRunner(
                settings,
                overload.run_id,
                args.timeout_seconds,
                args.poll_ms / 1000,
                overload.metadata,
                overload.stop_and_join,
                overload.writer.total_inserted,
                overload.writer.is_alive,
                fence_wakeup_mode=args.fence_wakeup_mode,
            ).run(False, require_nonzero_lag=True)
        except BaseException as error:
            flip_error = error
        inserted = 0
        if flip_error is not None:
            try:
                inserted = overload.stop_and_join(30.0)
            except BaseException as writer_error:
                raise RuntimeError(f"flip failed ({flip_error}); writer cleanup also failed ({writer_error})") from flip_error
        if flip_error is not None:
            raise flip_error
        assert result is not None
        inserted = int(result["workload_inserted_total"])
        print(
            json.dumps(
                {
                    "outcome": result["outcome"],
                    "run_id": result["run_id"],
                    "writer_park_ns": result["durations_ns"]["writer_park_ns"],
                    "inserted": inserted,
                },
                sort_keys=True,
            )
        )
    elif args.command == "benchmark-prodlike":
        if args.park_budget_ms <= 0 or args.revert_reserve_ms <= 0 or args.revert_reserve_ms >= args.park_budget_ms:
            raise ValueError("park budget must be positive and larger than its recovery reserve")
        workload = prepare_production_workload(
            settings,
            manifest,
            args.active_events_per_table,
            args.retiring_events_per_table,
            args.payload_bytes,
            args.active_pause_ms,
            args.retiring_pause_ms,
            args.max_source_lag_bytes,
            args.max_sink_lag_records_per_partition,
            args.stable_samples,
            args.max_batches,
            args.admission_timeout_seconds,
            args.poll_ms / 1000,
        )
        scenario = {
            **dict(workload.metadata),
            "park_budget_ms": args.park_budget_ms,
            "revert_reserve_ms": args.revert_reserve_ms,
            "forward_budget_ms": args.park_budget_ms - args.revert_reserve_ms,
        }
        flip_error: BaseException | None = None
        result = None
        try:
            result = FlipRunner(
                settings,
                workload.run_id,
                (args.park_budget_ms - args.revert_reserve_ms) / 1000,
                args.poll_ms / 1000,
                scenario,
                workload.stop_retiring,
                workload.writers.retiring_total,
                workload.writers.retiring_is_alive,
                workload.writers.active_total,
                workload.writers.active_is_alive,
                recovery_timeout_seconds=args.revert_reserve_ms / 1000,
                fence_wakeup_mode=args.fence_wakeup_mode,
            ).run(False)
        except BaseException as error:
            flip_error = error
        finally:
            try:
                workload.stop_retiring(30.0)
            finally:
                active_total = workload.stop_active(30.0)
        if flip_error is not None:
            raise flip_error
        assert result is not None
        print(
            json.dumps(
                {
                    "outcome": result["outcome"],
                    "run_id": result["run_id"],
                    "writer_park_ns": result["durations_ns"]["writer_park_ns"],
                    "active_inserted": active_total,
                    "retiring_inserted": result["workload_inserted_total"],
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
