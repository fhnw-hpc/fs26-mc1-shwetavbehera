"""
Experiment 3 — Consumer Sink Profiling + Bonus 4 (Bottleneck)
--------------------------------------------------------------
Profiles the message deserialization + processing loop of the consumer sink.
Also demonstrates a code-level bottleneck by comparing msgpack vs JSON
deserialization over many iterations.

Usage (from notebooks/ folder):
    docker exec -it jupyter1 python profiling/profile_consumer_sink.py

Output:
    profiling/results/consumer_msgpack.prof
    profiling/results/consumer_json.prof
    profiling/results/consumer_bottleneck.txt
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import cProfile
import pstats
import time
import random
import json
import statistics
from datetime import datetime, timezone
from io import StringIO

import msgpack
from shared.serializer import serialize, deserialize

ITERATIONS = 10000  # deserialize this many messages per experiment

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def make_sample_messages(n: int) -> list:
    """Generate n realistic stock tick messages as raw bytes (msgpack and json)."""
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


def simulate_consumer_msgpack(messages: list) -> list[float]:
    """
    Simulate the consumer processing loop using msgpack.
    This is the OPTIMIZED path (Bonus 1 from Part 1).
    """
    # Pre-serialize all messages as msgpack bytes (simulates receiving from Kafka)
    raw = [serialize(m) for m in messages]
    timings = []

    for payload in raw:
        t_start = time.perf_counter()

        # Deserialize
        data = deserialize(payload)

        # Simulate what the sink does: extract fields
        _ = {
            "symbol": data["symbol"],
            "price": data["price"],
            "volume": data["volume"],
            "timestamp": data["timestamp"],
        }

        t_end = time.perf_counter()
        timings.append((t_end - t_start) * 1000)

    return timings


def simulate_consumer_json(messages: list) -> list[float]:
    """
    Simulate the consumer processing loop using JSON.
    This is the BOTTLENECK path — demonstrates why we chose msgpack.
    Bonus 4: intentionally slower serializer to show the bottleneck.
    """
    # Pre-serialize all messages as JSON bytes
    raw = [json.dumps(m).encode("utf-8") for m in messages]
    timings = []

    for payload in raw:
        t_start = time.perf_counter()

        # Deserialize with JSON
        data = json.loads(payload.decode("utf-8"))

        _ = {
            "symbol": data["symbol"],
            "price": data["price"],
            "volume": data["volume"],
            "timestamp": data["timestamp"],
        }

        t_end = time.perf_counter()
        timings.append((t_end - t_start) * 1000)

    return timings


def run_bottleneck_experiment():
    """
    Experiment 3 + Bonus 4:
    Compare msgpack vs JSON deserialization performance.
    msgpack = mitigation, JSON = bottleneck.
    """
    print(f"\n=== Experiment 3 / Bonus 4: msgpack vs JSON Consumer ({ITERATIONS} iterations) ===")

    messages = make_sample_messages(ITERATIONS)

    # --- msgpack (optimized) ---
    t_msgpack = simulate_consumer_msgpack(messages)
    mean_mp = statistics.mean(t_msgpack)
    std_mp = statistics.stdev(t_msgpack)
    print(f"\n  msgpack (optimized):")
    print(f"    mean  = {mean_mp:.4f} ms")
    print(f"    stdev = {std_mp:.4f} ms")
    print(f"    total = {sum(t_msgpack):.2f} ms for {ITERATIONS} messages")

    # --- JSON (bottleneck) ---
    t_json = simulate_consumer_json(messages)
    mean_json = statistics.mean(t_json)
    std_json = statistics.stdev(t_json)
    print(f"\n  JSON (bottleneck):")
    print(f"    mean  = {mean_json:.4f} ms")
    print(f"    stdev = {std_json:.4f} ms")
    print(f"    total = {sum(t_json):.2f} ms for {ITERATIONS} messages")

    # Compute speedup
    speedup = mean_json / mean_mp
    print(f"\n  msgpack is {speedup:.2f}x faster than JSON per message")
    print(f"  At 10Hz over 1 hour: msgpack saves ~{(mean_json - mean_mp) * 36000:.0f} ms total")

    # Save results
    result_path = os.path.join(OUTPUT_DIR, "consumer_bottleneck.txt")
    with open(result_path, "w") as f:
        f.write(f"Consumer Deserialization Bottleneck Experiment\n")
        f.write(f"Iterations: {ITERATIONS}\n\n")
        f.write(f"msgpack:\n")
        f.write(f"  mean  = {mean_mp:.4f} ms\n")
        f.write(f"  stdev = {std_mp:.4f} ms\n")
        f.write(f"  total = {sum(t_msgpack):.2f} ms\n\n")
        f.write(f"JSON:\n")
        f.write(f"  mean  = {mean_json:.4f} ms\n")
        f.write(f"  stdev = {std_json:.4f} ms\n")
        f.write(f"  total = {sum(t_json):.2f} ms\n\n")
        f.write(f"Speedup: msgpack is {speedup:.2f}x faster than JSON\n")
    print(f"\n  Saved to: {result_path}")


def run_cprofile_msgpack():
    """Profile the msgpack consumer loop and save .prof file."""
    print(f"\n=== cProfile: msgpack consumer ===")
    messages = make_sample_messages(ITERATIONS)

    profiler = cProfile.Profile()
    profiler.enable()
    simulate_consumer_msgpack(messages)
    profiler.disable()

    prof_path = os.path.join(OUTPUT_DIR, "consumer_msgpack.prof")
    profiler.dump_stats(prof_path)
    print(f"  Saved to: {prof_path}")

    stream = StringIO()
    stats = pstats.Stats(profiler, stream=stream)
    stats.sort_stats("tottime")
    stats.print_stats(3)
    print("  Top 3 by total time:")
    print(stream.getvalue())


def run_cprofile_json():
    """Profile the JSON consumer loop and save .prof file — the bottleneck."""
    print(f"\n=== cProfile: JSON consumer (bottleneck) ===")
    messages = make_sample_messages(ITERATIONS)

    profiler = cProfile.Profile()
    profiler.enable()
    simulate_consumer_json(messages)
    profiler.disable()

    prof_path = os.path.join(OUTPUT_DIR, "consumer_json.prof")
    profiler.dump_stats(prof_path)
    print(f"  Saved to: {prof_path}")

    stream = StringIO()
    stats = pstats.Stats(profiler, stream=stream)
    stats.sort_stats("tottime")
    stats.print_stats(3)
    print("  Top 3 by total time:")
    print(stream.getvalue())


if __name__ == "__main__":
    run_bottleneck_experiment()
    run_cprofile_msgpack()
    run_cprofile_json()
    print("\nDone. View profiles with SnakeViz:")
    print("  snakeviz profiling/results/consumer_msgpack.prof")
    print("  snakeviz profiling/results/consumer_json.prof")