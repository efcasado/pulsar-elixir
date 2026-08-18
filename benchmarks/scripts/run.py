#!/usr/bin/env python3

import argparse
import json
import resource
import shlex
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]


def parse_bool(value: str) -> bool:
    normalized = value.lower()

    if normalized in {"1", "true", "yes", "on"}:
        return True

    if normalized in {"0", "false", "no", "off"}:
        return False

    raise argparse.ArgumentTypeError(f"invalid boolean value: {value}")


def add_shared_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--client", choices=["elixir", "go", "java"], default="elixir")
    parser.add_argument("--url", default="pulsar://localhost:6650")
    parser.add_argument("--admin-url")
    parser.add_argument("--messages", type=positive_int, default=100_000)
    parser.add_argument("--size", type=nonnegative_int, default=1_024)
    parser.add_argument("--partitions", type=positive_int, default=1)
    parser.add_argument("--in-flight", type=positive_int, default=1_000)
    parser.add_argument("--timeout-ms", type=positive_int, default=30_000)
    parser.add_argument("--batching", nargs="?", const=True, default=False, type=parse_bool)
    parser.add_argument("--batch-size", type=positive_int, default=100)
    parser.add_argument("--batch-delay-ms", type=positive_int, default=10)
    parser.add_argument("--compression", choices=["none", "zstd"], default="none")
    parser.add_argument(
        "--payload",
        dest="payload_mode",
        choices=["zero", "high-entropy"],
        default="zero",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the resolved command without connecting to Pulsar",
    )


def positive_int(value: str) -> int:
    parsed = int(value)

    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")

    return parsed


def nonnegative_int(value: str) -> int:
    parsed = int(value)

    if parsed < 0:
        raise argparse.ArgumentTypeError("must not be negative")

    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="./benchmarks/run",
        description="Run reproducible Pulsar client benchmarks",
    )
    subparsers = parser.add_subparsers(dest="operation", required=True)

    producer = subparsers.add_parser("producer", help="benchmark producer acknowledgements")
    add_shared_options(producer)
    producer.add_argument("--topic", default="persistent://public/default/benchmark-producer")

    consumer = subparsers.add_parser(
        "consumer", help="benchmark consumer delivery and acknowledgements"
    )
    add_shared_options(consumer)
    consumer.add_argument("--topic", default="persistent://public/default/benchmark-consumer")
    consumer.add_argument(
        "--subscription-type",
        choices=["shared", "exclusive", "failover", "key-shared"],
        default="shared",
    )
    consumer.add_argument("--subscription-name")
    consumer.add_argument("--consumers-per-partition", type=positive_int, default=1)

    return parser


def benchmark_args(args: argparse.Namespace) -> list[str]:
    command = [
        "--url",
        args.url,
        "--topic",
        args.topic,
        "--messages",
        str(args.messages),
        "--size",
        str(args.size),
        "--partitions",
        str(args.partitions),
        "--in-flight",
        str(args.in_flight),
        "--timeout-ms",
        str(args.timeout_ms),
        "--batch-size",
        str(args.batch_size),
        "--batch-delay-ms",
        str(args.batch_delay_ms),
        "--compression",
        args.compression,
        "--payload",
        args.payload_mode,
    ]

    if args.batching:
        if args.client == "go":
            command.append("--batching=true")
        elif args.client == "elixir":
            command.append("--batching")
        else:
            command.extend(["--batching", "true"])

    return command

def consumer_args(args: argparse.Namespace) -> list[str]:
    return [
        "--url",
        args.url,
        "--topic",
        args.topic,
        "--messages",
        str(args.messages),
        "--size",
        str(args.size),
        "--partitions",
        str(args.partitions),
        "--timeout-ms",
        str(args.timeout_ms),
        "--subscription-type",
        args.subscription_type,
        "--subscription-name",
        args.subscription_name,
        "--consumers-per-partition",
        str(args.consumers_per_partition),
    ]


def command_for(args: argparse.Namespace) -> list[str]:
    if args.operation == "consumer":
        runner_args = consumer_args(args)

        if args.client == "elixir":
            return [
                "mix",
                "run",
                "--no-compile",
                "benchmarks/elixir/consumer.exs",
                "--",
                *runner_args,
            ]

        if args.client == "go":
            return ["go", "-C", "benchmarks/go", "run", "./consumer", *runner_args]

        java_args = shlex.join(runner_args)
        return [
            "mise",
            "exec",
            "maven",
            "--",
            "mvn",
            "-q",
            "-f",
            "benchmarks/java/pom.xml",
            "compile",
            "exec:java",
            "-Dexec.mainClass=ConsumerBenchmark",
            f"-Dexec.args={java_args}",
        ]

    runner_args = benchmark_args(args)

    if args.client == "elixir":
        return [
            "mix",
            "run",
            "--no-compile",
            "benchmarks/elixir/producer.exs",
            "--",
            *runner_args,
        ]

    if args.client == "go":
        return ["go", "-C", "benchmarks/go", "run", ".", *runner_args]

    java_args = shlex.join(runner_args)
    return [
        "mise",
        "exec",
        "maven",
        "--",
        "mvn",
        "-q",
        "-f",
        "benchmarks/java/pom.xml",
        "compile",
        "exec:java",
        "-Dexec.mainClass=ProducerBenchmark",
        f"-Dexec.args={java_args}",
    ]

def admin_url_for(service_url: str, explicit_admin_url: str | None) -> str:
    if explicit_admin_url:
        return explicit_admin_url.rstrip("/")

    parsed = urlparse(service_url)
    if parsed.hostname is None:
        raise RuntimeError(f"cannot derive admin URL from service URL: {service_url}")

    admin_port = {6650: 8080, 6651: 8081}.get(parsed.port, 8080)
    scheme = "https" if parsed.scheme == "pulsar+ssl" else "http"
    return f"{scheme}://{parsed.hostname}:{admin_port}"


def partition_metadata_url(admin_url: str, topic: str) -> str:
    try:
        scheme, resource = topic.split("://", 1)
        tenant, namespace, name = resource.split("/", 2)
    except ValueError as error:
        raise RuntimeError(
            f"topic must use persistent://tenant/namespace/name form: {topic}"
        ) from error

    encoded = "/".join(
        quote(part, safe="") for part in (scheme, tenant, namespace, name)
    )
    return f"{admin_url.rstrip('/')}/admin/v2/{encoded}/partitions"


def request_partition_metadata(url: str, timeout_seconds: float) -> int | None:
    try:
        with urlopen(Request(url, method="GET"), timeout=timeout_seconds) as response:
            metadata = json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        if error.code == 404:
            return None
        raise RuntimeError(f"read partition metadata failed with HTTP {error.code}") from error
    except (URLError, OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"read partition metadata failed: {error}") from error

    if not isinstance(metadata, dict) or not isinstance(metadata.get("partitions"), int):
        raise RuntimeError(f"invalid partition metadata response: {metadata!r}")

    return metadata["partitions"]


def ensure_partitioned_topic(args: argparse.Namespace) -> None:
    metadata_url = partition_metadata_url(
        admin_url_for(args.url, args.admin_url),
        args.topic,
    )
    timeout_seconds = max(args.timeout_ms / 1000, 1)
    existing = request_partition_metadata(metadata_url, timeout_seconds)

    if existing is not None:
        if existing != args.partitions:
            raise RuntimeError(
                f"topic {args.topic} has {existing} partitions; "
                f"requested {args.partitions}"
            )
        return

    request = Request(
        metadata_url,
        data=json.dumps(args.partitions).encode("utf-8"),
        headers={"Content-Type": "application/vnd.partitioned-topic-metadata+json"},
        method="PUT",
    )
    try:
        with urlopen(request, timeout=timeout_seconds):
            return
    except HTTPError as error:
        if error.code == 409:
            raise RuntimeError(
                f"topic {args.topic} already exists and is not a matching "
                f"{args.partitions}-partition topic"
            ) from error
        raise RuntimeError(
            f"create partitioned topic failed with HTTP {error.code}"
        ) from error
    except (URLError, OSError) as error:
        raise RuntimeError(f"create partitioned topic failed: {error}") from error

def payload_bytes(size: int, mode: str) -> bytes:
    if mode == "zero":
        return bytes(size)

    state = 0xA5A5A5A5
    payload = bytearray(size)
    for index in range(size):
        state ^= (state << 13) & 0xFFFFFFFF
        state ^= state >> 17
        state = (state ^ ((state << 5) & 0xFFFFFFFF)) & 0xFFFFFFFF
        payload[index] = state >> 24

    return bytes(payload)


def prepare_backlog(args: argparse.Namespace) -> dict:
    try:
        import pulsar
    except ImportError as error:
        raise RuntimeError(
            "consumer preparation requires pulsar-client; "
            "install benchmarks/python/requirements.txt"
        ) from error

    started_at = time.monotonic()
    client = None
    producer = None

    try:
        client = pulsar.Client(
            args.url,
            operation_timeout_seconds=max((args.timeout_ms + 999) // 1000, 1),
            connection_timeout_ms=args.timeout_ms,
        )
        producer = client.create_producer(
            args.topic,
            producer_name=f"benchmark-preparer-{args.subscription_name}",
            send_timeout_millis=args.timeout_ms,
            compression_type=(
                pulsar.CompressionType.ZSTD
                if args.compression == "zstd"
                else pulsar.CompressionType.NONE
            ),
            max_pending_messages=args.in_flight,
            block_if_queue_full=True,
            batching_enabled=args.batching,
            batching_max_messages=args.batch_size,
            batching_max_publish_delay_ms=args.batch_delay_ms,
        )

        payload = payload_bytes(args.size, args.payload_mode)
        for sequence in range(args.messages):
            properties = {
                "benchmark-run": args.subscription_name,
                "benchmark-sequence": str(sequence),
                "benchmark-publish-ns": str(time.time_ns()),
            }
            partition_key = (
                f"benchmark-key-{sequence % max(args.partitions, 1)}"
                if args.subscription_type == "key-shared"
                else None
            )
            producer.send(
                payload,
                properties=properties,
                partition_key=partition_key,
            )

        producer.flush()
    except Exception as error:
        raise RuntimeError(f"prepare consumer backlog failed: {error}") from error
    finally:
        if producer is not None:
            producer.close()
        if client is not None:
            client.close()

    return {
        "messages_prepared": args.messages,
        "preparation_duration_us": max(
            int((time.monotonic() - started_at) * 1_000_000),
            1,
        ),
        "preparation_client": "python",
    }


def child_cpu_seconds(
    before: resource.struct_rusage, after: resource.struct_rusage
) -> float:
    return (
        after.ru_utime
        + after.ru_stime
        - before.ru_utime
        - before.ru_stime
    )


def peak_rss_mb(usage: resource.struct_rusage) -> float:
    # macOS reports ru_maxrss in bytes; Linux reports it in KiB.
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    return round(usage.ru_maxrss / divisor, 3)


def parse_result(stdout: str) -> dict:
    for line in reversed(stdout.splitlines()):
        line = line.strip()

        if line.startswith("{"):
            return json.loads(line)

    raise RuntimeError("benchmark runner did not emit a JSON result")


def run_child(args: argparse.Namespace, command: list[str]) -> dict:
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    started_at = time.monotonic()
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        text=True,
        capture_output=True,
    )
    duration_seconds = time.monotonic() - started_at
    after = resource.getrusage(resource.RUSAGE_CHILDREN)

    if completed.returncode != 0:
        if completed.stderr:
            sys.stderr.write(completed.stderr)
        raise RuntimeError(f"benchmark runner exited with status {completed.returncode}")

    result = parse_result(completed.stdout)
    if result.get("partitions") != args.partitions:
        raise RuntimeError(
            f"benchmark runner reported partitions={result.get('partitions')!r}; "
            f"expected {args.partitions}"
        )
    if args.operation == "consumer":
        if result.get("messages_received") != args.messages:
            raise RuntimeError(
                f"consumer runner reported messages_received={result.get('messages_received')!r}; "
                f"expected {args.messages}"
            )
        if result.get("messages_acked") != args.messages:
            raise RuntimeError(
                f"consumer runner reported messages_acked={result.get('messages_acked')!r}; "
                f"expected {args.messages}"
            )
    result.update(
        {
            "cpu_seconds": round(child_cpu_seconds(before, after), 3),
            "peak_rss_mb": peak_rss_mb(after),
            "wall_seconds": round(duration_seconds, 3),
            "resource_scope": "benchmark child process",
            "resource_source": "getrusage",
        }
    )

    return result


def run_producer(args: argparse.Namespace) -> dict:
    command = command_for(args)

    if args.dry_run:
        return {
            "schema_version": 1,
            "operation": args.operation,
            "client": args.client,
            "partitions": args.partitions,
            "admin_url": admin_url_for(args.url, args.admin_url),
            "compression": args.compression,
            "payload_mode": args.payload_mode,
            "dry_run": True,
            "command": command,
        }

    ensure_partitioned_topic(args)
    return run_child(args, command)


def run_consumer(args: argparse.Namespace) -> dict:
    command = command_for(args)

    if args.dry_run:
        return {
            "schema_version": 1,
            "operation": args.operation,
            "client": args.client,
            "partitions": args.partitions,
            "admin_url": admin_url_for(args.url, args.admin_url),
            "compression": args.compression,
            "payload_mode": args.payload_mode,
            "subscription_type": args.subscription_type,
            "subscription_name": args.subscription_name,
            "consumers_per_partition": args.consumers_per_partition,
            "messages_to_prepare": args.messages,
            "dry_run": True,
            "command": command,
        }

    ensure_partitioned_topic(args)
    preparation = prepare_backlog(args)
    result = run_child(args, command)
    result.update(
        {
            "subscription_type": args.subscription_type,
            "subscription_name": args.subscription_name,
            "consumers_per_partition": args.consumers_per_partition,
            "compression": args.compression,
            "payload_mode": args.payload_mode,
            "batching": args.batching,
            "batch_size": args.batch_size,
            "batch_delay_ms": args.batch_delay_ms,
            **preparation,
        }
    )
    return result


def run_benchmark(args: argparse.Namespace) -> dict:
    if args.operation == "consumer":
        args.subscription_name = args.subscription_name or f"benchmark-consumer-{uuid.uuid4().hex}"
        return run_consumer(args)

    return run_producer(args)

def write_result(result: dict, output: Path | None) -> None:
    encoded = json.dumps(result, sort_keys=True)

    if output is None:
        print(encoded)
        return

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(encoded + "\n", encoding="utf-8")
    print(encoded)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        write_result(run_benchmark(args), args.output)
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        parser.error(str(error))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
