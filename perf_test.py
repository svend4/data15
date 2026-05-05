#!/usr/bin/env python3
"""
Performance Test for Orchestrator v5.0
Tests without concurrent writes to avoid JSON lock issues
"""
import time
import json
import sys
sys.path.insert(0, '/workspace/orchestrator')

from orchestrator_v5 import (
    ConfigManager, CacheManager, RateLimiter, EventBus,
    BoardManager, get_utc_timestamp
)

def test_sequential_tasks(board, count=100):
    """Add tasks sequentially"""
    print(f"\n📊 Adding {count} tasks sequentially...")
    start = time.time()

    for i in range(count):
        board.add_task(
            title=f"Perf Task {i}",
            task_type="analysis",
            priority="medium"
        )

    elapsed = time.time() - start
    print(f"✅ Added {count} tasks in {elapsed:.2f}s ({count/elapsed:.1f} tasks/sec)")
    return elapsed

def test_cache_performance(cache, count=1000):
    """Test cache with repeated queries"""
    print(f"\n💾 Testing cache with {count} queries...")

    queries = [
        "Tesla stock",
        "AI trends",
        "Python optimization",
        "Market data",
        "System monitoring"
    ] * (count // 5)

    start = time.time()
    hits = 0
    misses = 0

    for query in queries:
        if cache.get(query, "analyze"):
            hits += 1
        else:
            cache.set(query, f"Result for: {query}", "analyze")
            misses += 1

    elapsed = time.time() - start
    hit_rate = (hits / count) * 100 if count > 0 else 0

    print(f"✅ Cache: {hits} hits, {misses} misses ({hit_rate:.1f}% hit rate)")
    print(f"   Time: {elapsed:.3f}s ({count/elapsed:.0f} ops/sec)")

    return hit_rate, elapsed

def test_rate_limiting(rate_limiter, count=500):
    """Test rate limiter"""
    print(f"\n⚡ Rate limiting test ({count} requests)...")

    allowed = 0
    blocked = 0
    start = time.time()

    for i in range(count):
        key = f"user_{i % 20}"
        if rate_limiter.is_allowed(key):
            allowed += 1
        else:
            blocked += 1

    elapsed = time.time() - start
    print(f"✅ {allowed} allowed, {blocked} blocked")
    print(f"   Time: {elapsed:.3f}s ({count/elapsed:.0f} ops/sec)")

    return allowed, blocked

def test_event_bus(events, count=1000):
    """Test event publishing"""
    print(f"\n📡 Event bus test ({count} events)...")

    start = time.time()
    for i in range(count):
        events.publish("test.event", {"id": i, "data": f"Event {i}"})

    elapsed = time.time() - start
    print(f"✅ {count} events in {elapsed:.3f}s ({count/elapsed:.0f} ops/sec)")
    return elapsed

def main():
    print("=" * 60)
    print("🚀 ORCHESTRATOR v5.0 PERFORMANCE TEST")
    print("=" * 60)

    # Initialize
    config = ConfigManager()
    cache = CacheManager()
    rate_limiter = RateLimiter(max_requests=100, window_seconds=60)
    events = EventBus()
    board = BoardManager()

    # Test 1: Sequential tasks (no concurrency)
    task_time = test_sequential_tasks(board, count=20)

    # Test 2: Cache
    hit_rate, cache_time = test_cache_performance(cache, count=1000)

    # Test 3: Rate limiting
    allowed, blocked = test_rate_limiting(rate_limiter, count=500)

    # Test 4: Event bus
    event_time = test_event_bus(events, count=1000)

    # Summary
    print("\n" + "=" * 60)
    print("📊 PERFORMANCE SUMMARY")
    print("=" * 60)
    print(f"Task Creation:    {20/task_time:.1f} tasks/sec")
    print(f"Cache Ops:        {1000/cache_time:.0f} ops/sec ({hit_rate:.1f}% hit rate)")
    print(f"Rate Limiter:     {500/rate_limiter_max_time:.0f} ops/sec")
    print(f"Event Bus:        {1000/event_time:.0f} ops/sec")
    print("=" * 60)

    # Board stats
    stats = board.get_stats()
    print(f"\n📋 Board: {stats['total']} tasks total")

    # Cache stats
    cache_stats = cache.get_stats()
    print(f"💾 Cache: {cache_stats['entries']} entries, {cache_stats['hit_rate']:.1f}% hit rate")

if __name__ == "__main__":
    main()
