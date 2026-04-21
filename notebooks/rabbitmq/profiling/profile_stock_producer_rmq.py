"""
Experiment 1 & 2 — RabbitMQ Stock Producer Profiling
------------------------------------------------------
Same experiment as profile_stock_producer.py but for RabbitMQ.
Run this to compare Kafka vs RabbitMQ producer performance.

Usage (from notebooks/rabbitmq/ folder):
    docker exec -it rmq-stock-producer python /app/profiling/profile_stock_producer_rmq.py

Or locally:
    RABBITMQ_HOST=localhost python profiling/profile_stock_producer_rmq.py

Output:
    profiling/results/rmq_producer.prof
    profiling/results/rmq_producer_timing.txt
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../rabbitmq'))

import cProfile
import pstats
import time
import random
import statistics
from datetime import datetime, timezone
from io import StringIO

import pika
from rabbitmq.shared.rmq_config import EXCHANGES, RABBITMQ_HOST
from rabbitmq.shared.rmq_helpers import get_connection, declare_exchange
from rabbitmq.shared.serializer import serialize

EXCHANGE = EXCHANGES["stock_ticks"]
ITERATIONS = 500
RUNS = 5

STOCKS = {
    "AAPL": 182.0, "GOOGL": 140.0, "MSFT": 415.0,
    "TSLA": 175.0, "AMZN": 185.0,
}

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "results")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def next_price(current: float) -> float:
    return round(max(1.0, current + random.uniform(-0.5, 0.5)), 2)


def producer_loop(channel, iterations: int) -> list[float]:
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
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key="",
            body=serialize(message),
            properties=pika.BasicProperties(delivery_mode=2),
        )

        t_end = time.perf_counter()
        timings.append((t_end - t_start) * 1000)

    return timings


def run_timing_experiment():
    print(f"\n=== Experiment 1: RabbitMQ Producer Timing ({RUNS} runs x {ITERATIONS} iterations) ===")
    connection = get_connection()
    channel = connection.channel()
    declare_exchange(channel, EXCHANGE)

    all_run_means = []
    all_timings = []

    for run in range(1, RUNS + 1):
        timings = producer_loop(channel, ITERATIONS)
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

    result_path = os.path.join(OUTPUT_DIR, "rmq_producer_timing.txt")
    with open(result_path, "w") as f:
        f.write(f"RabbitMQ Stock Producer Timing\n")
        f.write(f"Runs: {RUNS} x {ITERATIONS} iterations each\n\n")
        for i, m in enumerate(all_run_means, 1):
            f.write(f"Run {i} mean: {m:.3f} ms\n")
        f.write(f"\nOverall mean:  {overall_mean:.3f} ms\n")
        f.write(f"Overall stdev: {overall_std:.3f} ms\n")
        f.write(f"Overall min:   {overall_min:.3f} ms\n")
        f.write(f"Overall max:   {overall_max:.3f} ms\n")
    print(f"\n  Saved timing results to: {result_path}")

    connection.close()
    return overall_mean, overall_std


def run_cprofile_experiment():
    print(f"\n=== Experiment 2: RabbitMQ Producer cProfile ({ITERATIONS} iterations) ===")
    connection = get_connection()
    channel = connection.channel()
    declare_exchange(channel, EXCHANGE)

    profiler = cProfile.Profile()
    profiler.enable()
    producer_loop(channel, ITERATIONS)
    profiler.disable()
    connection.close()

    prof_path = os.path.join(OUTPUT_DIR, "rmq_producer.prof")
    profiler.dump_stats(prof_path)
    print(f"  Saved profile to: {prof_path}")

    stream = StringIO()
    stats = pstats.Stats(profiler, stream=stream)
    stats.sort_stats("cumulative")
    stats.print_stats(10)
    print("\n  Top 10 functions by cumulative time:")
    print(stream.getvalue())

    stream2 = StringIO()
    stats2 = pstats.Stats(profiler, stream=stream2)
    stats2.sort_stats("tottime")
    stats2.print_stats(3)
    print("  Top 3 functions by total time:")
    print(stream2.getvalue())


if __name__ == "__main__":
    run_timing_experiment()
    run_cprofile_experiment()
    print("\nDone.")