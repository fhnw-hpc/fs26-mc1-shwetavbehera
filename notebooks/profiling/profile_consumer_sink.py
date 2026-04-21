"""
Experiment 3  Consumer Sink Profiling + (Bottleneck)
--------------------------------------------------------------
Profiles the full consumer sink callback: deserialization + field
extraction + file open + CSV write + flush.

Also compares msgpack vs JSON deserialization in isolation to show
the serialization speedup from Bonus 1 (Part 1).

Usage (from notebooks/ folder):
    docker exec -it jupyter1 python profiling/profile_consumer_sink.py
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import cProfile
import pstats
import time
import random
import json
import csv
import statistics
import tempfile
from datetime import datetime, timezone
from io import StringIO

import msgpack
from shared.serializer import serialize, deserialize

ITERATIONS = 10000

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)

FIELDS = ["symbol", "price", "volume", "timestamp"]


def make_sample_messages(n: int) -> list:
    symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN"]
    messages = []
    for _ in range(n):
        msg = {
            "symbol": random.choice(symbols),
            "price": round(random.uniform(100, 500), 2),
            "volume": random.randint(10, 500),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        messages.append(msg)
    return messages


# ── Deserialization-only timings (original experiment) ───────────────────────

def simulate_consumer_msgpack(messages: list) -> list:
    """Deserialization + field extraction only — no file I/O."""
    raw = [serialize(m) for m in messages]
    timings = []
    for payload in raw:
        t0 = time.perf_counter()
        data = deserialize(payload)
        _ = {k: data[k] for k in FIELDS}
        timings.append((time.perf_counter() - t0) * 1000)
    return timings


def simulate_consumer_json(messages: list) -> list:
    """JSON deserialization + field extraction only — the bottleneck path."""
    raw = [json.dumps(m).encode("utf-8") for m in messages]
    timings = []
    for payload in raw:
        t0 = time.perf_counter()
        data = json.loads(payload.decode("utf-8"))
        _ = {k: data[k] for k in FIELDS}
        timings.append((time.perf_counter() - t0) * 1000)
    return timings


# ── Full sink callback timings ─────────────────────

def simulate_full_sink_callback(messages: list) -> list:
    """
    Simulate the FULL consumer_sink_stocks callback:
      deserialize → extract fields → open CSV → write row → flush

    This is what actually runs in production per message.
    Uses a temp file so it doesn't pollute real data.
    """
    raw = [serialize(m) for m in messages]
    timings = []

    with tempfile.NamedTemporaryFile(mode="a", suffix=".csv",
                                     delete=False, newline="") as tmp:
        tmp_path = tmp.name

    file_exists = False
    for payload in raw:
        t0 = time.perf_counter()

        # Step 1: deserialize (same as production)
        data = deserialize(payload)

        # Step 2: extract fields (same as production)
        row = {k: data.get(k) for k in FIELDS}

        # Step 3: open file, write row, flush (same as production)
        with open(tmp_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            if not file_exists:
                writer.writeheader()
                file_exists = True
            writer.writerow(row)
            f.flush()

        timings.append((time.perf_counter() - t0) * 1000)

    # Clean up temp file
    os.unlink(tmp_path)
    return timings


def simulate_full_sink_callback_batched(messages: list, batch_size: int = 100) -> list:
    """
    Mitigation: same sink but flush every batch_size rows instead of every row.
    Shows what happens if we fix the bottleneck.
    """
    raw = [serialize(m) for m in messages]
    timings = []
    buffer = []

    with tempfile.NamedTemporaryFile(mode="a", suffix=".csv",
                                     delete=False, newline="") as tmp:
        tmp_path = tmp.name

    file_exists = False
    for i, payload in enumerate(raw):
        t0 = time.perf_counter()

        data = deserialize(payload)
        row = {k: data.get(k) for k in FIELDS}
        buffer.append(row)

        # Only write and flush when buffer is full
        if len(buffer) >= batch_size:
            with open(tmp_path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDS)
                if not file_exists:
                    writer.writeheader()
                    file_exists = True
                writer.writerows(buffer)
                f.flush()
            buffer.clear()

        timings.append((time.perf_counter() - t0) * 1000)

    # Flush remaining
    if buffer:
        with open(tmp_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writerows(buffer)
            f.flush()

    os.unlink(tmp_path)
    return timings


# ── Experiments ───────────────────────────────────────────────────────────────

def run_bottleneck_experiment():
    print(f"\n=== Deserialization only: msgpack vs JSON ({ITERATIONS} iterations) ===")
    messages = make_sample_messages(ITERATIONS)

    t_mp = simulate_consumer_msgpack(messages)
    t_js = simulate_consumer_json(messages)
    speedup = statistics.mean(t_js) / statistics.mean(t_mp)

    print(f"\n  msgpack:  mean={statistics.mean(t_mp):.4f}ms  stdev={statistics.stdev(t_mp):.4f}ms  total={sum(t_mp):.2f}ms")
    print(f"  JSON:     mean={statistics.mean(t_js):.4f}ms  stdev={statistics.stdev(t_js):.4f}ms  total={sum(t_js):.2f}ms")
    print(f"  Speedup:  msgpack is {speedup:.2f}x faster than JSON")
    return t_mp, t_js, speedup


def run_full_sink_experiment():
    print(f"\n=== Full sink callback: per-message flush vs batched ({ITERATIONS} iterations) ===")
    messages = make_sample_messages(ITERATIONS)

    print("  Running per-message flush (production behaviour)...")
    t_single = simulate_full_sink_callback(messages)

    print("  Running batched flush (100 messages per flush, mitigation)...")
    t_batch = simulate_full_sink_callback_batched(messages, batch_size=100)

    mean_s = statistics.mean(t_single)
    mean_b = statistics.mean(t_batch)
    speedup = mean_s / mean_b

    print(f"\n  Per-message flush:  mean={mean_s:.4f}ms  stdev={statistics.stdev(t_single):.4f}ms  total={sum(t_single):.2f}ms")
    print(f"  Batched flush:      mean={mean_b:.4f}ms  stdev={statistics.stdev(t_batch):.4f}ms  total={sum(t_batch):.2f}ms")
    print(f"  Speedup:  batched is {speedup:.2f}x faster per message")
    print(f"\n  File I/O contribution per message:")
    print(f"    Deserialize only (msgpack): {statistics.mean(simulate_consumer_msgpack(messages)):.4f}ms")
    print(f"    Full callback (per-flush):  {mean_s:.4f}ms")
    print(f"    => File open+write+flush adds ~{mean_s - statistics.mean(simulate_consumer_msgpack(messages)):.4f}ms per message")

    return t_single, t_batch, speedup


def run_cprofile_msgpack():
    print(f"\n=== cProfile: msgpack consumer (deserialization only) ===")
    messages = make_sample_messages(ITERATIONS)
    profiler = cProfile.Profile()
    profiler.enable()
    simulate_consumer_msgpack(messages)
    profiler.disable()
    prof_path = os.path.join(OUTPUT_DIR, "consumer_msgpack.prof")
    profiler.dump_stats(prof_path)
    stream = StringIO()
    pstats.Stats(profiler, stream=stream).sort_stats("tottime").print_stats(3)
    print(f"  Saved to: {prof_path}")
    print("  Top 3 by total time:")
    print(stream.getvalue())


def run_cprofile_json():
    print(f"\n=== cProfile: JSON consumer (bottleneck) ===")
    messages = make_sample_messages(ITERATIONS)
    profiler = cProfile.Profile()
    profiler.enable()
    simulate_consumer_json(messages)
    profiler.disable()
    prof_path = os.path.join(OUTPUT_DIR, "consumer_json.prof")
    profiler.dump_stats(prof_path)
    stream = StringIO()
    pstats.Stats(profiler, stream=stream).sort_stats("tottime").print_stats(3)
    print(f"  Saved to: {prof_path}")
    print("  Top 3 by total time:")
    print(stream.getvalue())


def save_results(t_mp, t_js, deser_speedup, t_single, t_batch, io_speedup):
    result_path = os.path.join(OUTPUT_DIR, "consumer_bottleneck.txt")
    with open(result_path, "w") as f:
        f.write("Consumer Sink Profiling Results\n")
        f.write(f"Iterations: {ITERATIONS}\n\n")

        f.write("--- Deserialization only ---\n")
        f.write(f"msgpack:  mean={statistics.mean(t_mp):.4f}ms  stdev={statistics.stdev(t_mp):.4f}ms  total={sum(t_mp):.2f}ms\n")
        f.write(f"JSON:     mean={statistics.mean(t_js):.4f}ms  stdev={statistics.stdev(t_js):.4f}ms  total={sum(t_js):.2f}ms\n")
        f.write(f"Speedup:  {deser_speedup:.2f}x\n\n")

        f.write("--- Full sink callback (deserialize + file open + write + flush) ---\n")
        f.write(f"Per-message flush:  mean={statistics.mean(t_single):.4f}ms  stdev={statistics.stdev(t_single):.4f}ms  total={sum(t_single):.2f}ms\n")
        f.write(f"Batched flush:      mean={statistics.mean(t_batch):.4f}ms  stdev={statistics.stdev(t_batch):.4f}ms  total={sum(t_batch):.2f}ms\n")
        f.write(f"Speedup:  {io_speedup:.2f}x\n")
    print(f"\n  Results saved to: {result_path}")


if __name__ == "__main__":
    t_mp, t_js, deser_speedup = run_bottleneck_experiment()
    t_single, t_batch, io_speedup = run_full_sink_experiment()
    run_cprofile_msgpack()
    run_cprofile_json()
    save_results(t_mp, t_js, deser_speedup, t_single, t_batch, io_speedup)

    print("\nDone. View profiles with SnakeViz:")
    print("  snakeviz profiling/results/consumer_msgpack.prof")
    print("  snakeviz profiling/results/consumer_json.prof")