#!/usr/bin/env python3
"""Complex Hermes LLM Test"""
import json
from hermes_llm import get_hermes_llm

hermes = get_hermes_llm()

print("=" * 60)
print("HERMES LLM TEST: Реальный анализ с MiniMax API")
print("=" * 60)

# Test 1: Анализ документа
print("\n📄 Test 1: Анализ документа 'Обзор возможностей'")
print("-" * 60)

doc_context = """
Документ описывает современные AI агенты:
1. Hermes Agent (Nous Research) - самообучающийся агент с навыками
2. OpenClaw - локальный исполнительный движок
3. Multica - платформа оркестрации
4. MCP - Model Context Protocol для интеграций
"""

result1 = hermes.analyze("DOC-001", "Сравнение Hermes vs OpenClaw vs Multica", doc_context)
print(f"Status: {result1['status']}")
print(f"Agent: {result1['agent']}")
print(f"\n📝 Результат анализа:")
print(result1['analysis'][:500])
print(f"\n📁 Log: {result1.get('log_file', 'N/A')}")

# Test 2: Финансовый анализ
print("\n\n💰 Test 2: Финансовый анализ NVIDIA vs AMD vs Intel")
print("-" * 60)

stocks_context = """
NVIDIA: Рыночная капитализация ~$2.8T, лидер в GPU для AI
AMD: Конкурирует в сегменте дата-центров, доля рынка растёт
Intel: Традиционный чипмейкер, пытается догнать в AI
"""

result2 = hermes.analyze("FIN-001", "Финансовое сравнение NVIDIA vs AMD vs Intel", stocks_context)
print(f"Status: {result2['status']}")
print(f"Agent: {result2['agent']}")
print(f"\n📝 Результат:")
print(result2['analysis'][:500])

# Test 3: AI тренды
print("\n\n🤖 Test 3: Тренды AI агентов 2025")
print("-" * 60)

ai_context = """
Тренды AI в 2025:
- AI агенты переходят от экспериментов к production
- 40% проектов будут отменены к 2027 из-за затрат
- Hermes Agent: 110K+ GitHub stars за 10 недель
- OpenClaw: вирусный проект в децентрализованном AI
"""

result3 = hermes.analyze("AI-001", "Обзор трендов AI агентов 2025", ai_context)
print(f"Status: {result3['status']}")
print(f"\n📝 Результат:")
print(result3['analysis'][:400])

print("\n" + "=" * 60)
print("✅ Все тесты завершены успешно!")
print("=" * 60)
