#!/usr/bin/env python3
"""Complex Hermes LLM Test"""
import json
from hermes_llm import HermesLLMClient, LLMConfig

def test_hermes():
    config = LLMConfig()
    client = HermesLLMClient(config)
    
    print("=" * 60)
    print("HERMES LLM TEST: Complex Analysis")
    print("=" * 60)
    
    # Test 1: Document analysis
    print("\n📄 Test 1: Document Analysis")
    result1 = client.complete("Analyze AI agents comparison")
    print(f"Result: {result1['text'][:200]}...")
    
    # Test 2: Financial analysis
    print("\n💰 Test 2: Financial Analysis")
    result2 = client.complete("Compare NVIDIA vs AMD vs Intel")
    print(f"Result: {result2['text'][:200]}...")
    
    # Test 3: AI trends
    print("\n🤖 Test 3: AI Trends 2025")
    result3 = client.complete("Overview of AI agent trends 2025")
    print(f"Result: {result3['text'][:200]}...")
    
    print("\n✅ All tests completed!")

if __name__ == "__main__":
    test_hermes()
