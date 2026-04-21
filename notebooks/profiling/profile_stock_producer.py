"""
Experiment 1 & 2 — Kafka Stock Producer Profiling
---------------------------------------------------
Runs the stock producer loop for N iterations (no sleep),
measures per-loop timing, and saves a .prof file for SnakeViz.

Usage (from notebooks/ folder):
    docker exec -it jupyter1 python profiling/profile_stock_producer.py

Output:
    profiling/results/kafka_producer.prof   → open with SnakeViz
    profiling/results/kafka_producer_timing.txt → mean/stddev per loop
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import cProfile
import pstats
import time
import random
import statistics
from datetime import datetime, timezone
from io import StringIO

from kafka import KafkaProducer
from notebooks.shared.kafka_config import BOOTSTRAP_SERVERS, TOPICS
from notebooks.shared.serializer import serialize

TOPIC = TOPICS["stock_ticks"]
ITERATIONS = 500  # number of messages to send per run
RUNS = 5          # repeat the whole experiment this many times for stddev

STOCKS = {
    "AAPL": 182.0, "GOOGL": 140.0, "MSFT": 415.0,
    "TSLA": 175.0, "AMZN": 185.0,
}

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def next_price(current: float) -> float:
    return round(max(1.0, current + random.uniform(-0.5, 0.5)), 2)


def producer_loop(producer, iterations: int) -> list[float]:
    """Run the producer loop and return per-iteration timings in ms."""
    prices = dict(STOCKS)
    timings = []

    for _ in range(iterations):
        t_start = time.perf_counter()

        symbol = random.choice(list(prices.keys()))
        prices[symbol] = next_price(prices[symbol])
        message = {
            "symbol": symbol,
            "price": prices[symbol],
            "volume": random.randint(10, 500),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        producer.send(TOPIC, value=message)

        t_end = time.perf_counter()
        timings.append((t_end - t_start) * 1000)  # convert to ms

    producer.flush()
    return timings


def run_timing_experiment():
    """Experiment 1: measure mean and stddev of loop time over multiple runs."""
    print(f"\n=== Experiment 1: Kafka Producer Timing ({RUNS} runs x {ITERATIONS} iterations) ===")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=serialize,
    )

    all_run_means = []
    all_timings = []

    for run in range(1, RUNS + 1):
        timings = producer_loop(producer, ITERATIONS)
        run_mean = statistics.mean(timings)
        run_std = statistics.stdev(timings)
        all_run_means.append(run_mean)
        all_timings.extend(timings)
        print(f"  Run {run}: mean={run_mean:.3f}ms  stdev={run_std:.3f}ms")

    overall_mean = statistics.mean(all_timings)
    overall_std = statistics.stdev(all_timings)
    overall_min = min(all_timings)
    overall_max = max(all_timings)

    print(f"\n  Overall ({RUNS * ITERATIONS} iterations):")
    print(f"    mean  = {overall_mean:.3f} ms")
    print(f"    stdev = {overall_std:.3f} ms")
    print(f"    min   = {overall_min:.3f} ms")
    print(f"    max   = {overall_max:.3f} ms")

    # Save timing results to txt
    result_path = os.path.join(OUTPUT_DIR, "kafka_producer_timing.txt")
    with open(result_path, "w") as f:
        f.write(f"Kafka Stock Producer Timing\n")
        f.write(f"Runs: {RUNS} x {ITERATIONS} iterations each\n\n")
        for i, m in enumerate(all_run_means, 1):
            f.write(f"Run {i} mean: {m:.3f} ms\n")
        f.write(f"\nOverall mean:  {overall_mean:.3f} ms\n")
        f.write(f"Overall stdev: {overall_std:.3f} ms\n")
        f.write(f"Overall min:   {overall_min:.3f} ms\n")
        f.write(f"Overall max:   {overall_max:.3f} ms\n")
    print(f"\n  Saved timing results to: {result_path}")

    producer.close()
    return overall_mean, overall_std


def run_cprofile_experiment():
    """Experiment 2: cProfile the producer loop, save .prof for SnakeViz."""
    print(f"\n=== Experiment 2: Kafka Producer cProfile ({ITERATIONS} iterations) ===")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=serialize,
    )

    profiler = cProfile.Profile()
    profiler.enable()
    producer_loop(producer, ITERATIONS)
    profiler.disable()
    producer.close()

    # Save .prof file for SnakeViz
    prof_path = os.path.join(OUTPUT_DIR, "kafka_producer.prof")
    profiler.dump_stats(prof_path)
    print(f"  Saved profile to: {prof_path}")
    print(f"  View with: snakeviz {prof_path}")

    # Also print top 10 functions by cumulative time inline
    stream = StringIO()
    stats = pstats.Stats(profiler, stream=stream)
    stats.sort_stats("cumulative")
    stats.print_stats(10)
    print("\n  Top 10 functions by cumulative time:")
    print(stream.getvalue())

    # Print top 3 by total time (answers the assignment question directly)
    stream2 = StringIO()
    stats2 = pstats.Stats(profiler, stream=stream2)
    stats2.sort_stats("tottime")
    stats2.print_stats(3)
    print("  Top 3 functions by total time:")
    print(stream2.getvalue())


if __name__ == "__main__":
    run_timing_experiment()
    run_cprofile_experiment()
    print("\nDone. Run SnakeViz to visualize:")
    print("  pip install snakeviz")
    print("  snakeviz profiling/results/kafka_producer.prof")