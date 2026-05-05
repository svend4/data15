#!/usr/bin/env python3
"""
Stress Test for Orchestrator v5.0
Tests performance with large volumes of data
"""
import time
import threading
import json
import sys
import os
sys.path.insert(0, '/workspace/orchestrator')

from orchestrator_v5 import (
    ConfigManager, CacheManager, RateLimiter, EventBus,
    BoardManager, CAMELLayer, get_utc_timestamp
)

def test_concurrent_tasks(board, count=50):
    """Add many tasks concurrently"""
    print(f"\n📊 Adding {count} tasks concurrently...")
    start = time.time()

    def add_task(i):
        board.add_task(
            title=f"Stress Task {i}",
            task_type="analysis",
            priority="medium"
        )

    threads = []
    for i in range(count):
        t = threading.Thread(target=add_task, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    elapsed = time.time() - start
    print(f"✅ Added {count} tasks in {elapsed:.2f}s ({count/elapsed:.1f} tasks/sec)")
    return elapsed

def test_cache_performance(cache, count=100):
    """Test cache hit rate with repeated queries"""
    print(f"\n💾 Testing cache with {count} queries...")

    queries = [
        "Tesla stock analysis",
        "AI trends 2025",
        "Python optimization",
        "Market data",
        "System monitoring"
    ] * (count // 5)

    start = time.time()
    hits = 0
    misses = 0

    for i, query in enumerate(queries):
        key = cache._generate_key(query, "analyze")
        if cache.get(query, "analyze"):
            hits += 1
        else:
            cache.set(query, f"Result for: {query}", "analyze")
            misses += 1

    elapsed = time.time() - start
    hit_rate = (hits / count) * 100 if count > 0 else 0

    print(f"✅ Cache test: {hits} hits, {misses} misses")
    print(f"   Hit rate: {hit_rate:.1f}%")
    print(f"   Time: {elapsed:.3f}s ({count/elapsed:.0f} ops/sec)")

    return hit_rate, elapsed

def test_rate_limiting(rate_limiter, count=150):
    """Test rate limiter with many requests"""
    print(f"\n⚡ Testing rate limiting with {count} requests...")

    allowed = 0
    blocked = 0
    start = time.time()

    for i in range(count):
        key = f"user_test_{i % 10}"
        if rate_limiter.is_allowed(key):
            allowed += 1
        else:
            blocked += 1

    elapsed = time.time() - start
    print(f"✅ Rate limit test: {allowed} allowed, {blocked} blocked")
    print(f"   Time: {elapsed:.3f}s ({count/elapsed:.0f} ops/sec)")

    return allowed, blocked

def test_event_bus(events, count=100):
    """Test event publishing"""
    print(f"\n📡 Testing event bus with {count} events...")

    def publish_events():
        for i in range(count):
            events.publish("test.event", {"id": i, "data": f"Event {i}"})

    start = time.time()
    publish_events()
    elapsed = time.time() - start

    print(f"✅ Published {count} events in {elapsed:.3f}s ({count/elapsed:.0f} ops/sec)")
    print(f"   Subscribers: {len(events.subscribers)}")

    return elapsed

def main():
    print("=" * 60)
    print("🚀 ORCHESTRATOR v5.0 STRESS TEST")
    print("=" * 60)

    # Initialize components
    config = ConfigManager()
    cache = CacheManager()
    rate_limiter = RateLimiter(max_requests=100, window_seconds=60)
    events = EventBus()
    board = BoardManager()

    # Test 1: Concurrent task creation
    test_concurrent_tasks(board, count=50)

    # Test 2: Cache performance
    hit_rate, cache_time = test_cache_performance(cache, count=100)

    # Test 3: Rate limiting
    allowed, blocked = test_rate_limiting(rate_limiter, count=150)

    # Test 4: Event bus
    event_time = test_event_bus(events, count=100)

    # Summary
    print("\n" + "=" * 60)
    print("📈 STRESS TEST SUMMARY")
    print("=" * 60)
    print(f"Tasks created: 50 (concurrent)")
    print(f"Cache hit rate: {hit_rate:.1f}%")
    print(f"Rate limit (100/60s): {allowed} allowed, {blocked} blocked")
    print(f"Events published: 100")
    print("=" * 60)

    # Board stats
    stats = board.get_stats()
    print(f"\n📋 Board Stats: {stats}")

    # Cache stats
    cache_stats = cache.get_stats()
    print(f"\n💾 Cache Stats: {json.dumps(cache_stats, indent=2)}")

if __name__ == "__main__":
    main()
