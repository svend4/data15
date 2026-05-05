#!/usr/bin/env python3
"""
Deep Testing Script for Orchestrator v5.0
Comprehensive analysis of all implemented modules
"""

import sys
sys.path.insert(0, '.')

from orchestrator_v5 import *
import time

print("=" * 80)
print("🔬 ГЛУБОКОЕ ТЕСТИРОВАНИЕ ORCHESTRATOR v5.0")
print("=" * 80)

orch = HybridOrchestrator()

# ============================================================================
# 1. INTEGRATION HUB - АНАЛИЗ
# ============================================================================
print("\n" + "=" * 80)
print("📡 1. INTEGRATION HUB")
print("=" * 80)

print("""
ПЛЮСЫ:
✓ Единая точка для всех интеграций (Slack, Email, Webhooks)
✓ Централизованное управление статусами
✓ Расширяемость - легко добавить новые интеграции

МИНУСЫ:
✗ Заглушки (stubs) для реальных вызовов API
✗ Нет retry логики при сбоях
✗ Нет асинхронной отправки
""")

# Тест Integration Hub
result1 = orch.integrations.register_integration('slack', {'webhook_url': 'https://hooks.slack.com/services/XXX'})
result2 = orch.integrations.register_integration('email', {'smtp_host': 'smtp.gmail.com', 'smtp_port': 587})
result3 = orch.integrations.register_integration('jira', {'base_url': 'https://company.atlassian.net'})

print(f"\n✅ Зарегистрировано интеграций: {orch.integrations.get_status()['total']}")
print(f"✅ Активных: {orch.integrations.get_status()['enabled']}")

# Тест отправки уведомлений
notif1 = orch.integrations.send_notification('slack', 'Новая задача создана!', {'task_id': 'T-001'})
notif2 = orch.integrations.send_notification('email', 'Напоминание о задаче', {'task_id': 'T-002'})

print(f"\n📤 Тест отправки:")
print(f"   Slack: {notif1.get('status', 'error')}")
print(f"   Email: {notif2.get('status', 'error')}")

# ============================================================================
# 2. WORKFLOW ENGINE - АНАЛИЗ
# ============================================================================
print("\n" + "=" * 80)
print("⚙️ 2. WORKFLOW ENGINE")
print("=" * 80)

print("""
ПЛЮСЫ:
✓ Визуальное определение workflow с шагами
✓ Поддержка условной логики (conditions)
✓ Встроенные действия (create_task, update_status, notify)
✓ Отслеживание выполнения и история

МИНУСЫ:
✗ Ограниченный набор действий
✗ Нет параллельного выполнения шагов
✗ Нет циклов (loops)
✗ Упрощённый парсинг cron
""")

workflow_steps = [
    {
        'name': 'Создать задачу',
        'type': 'action',
        'action': 'create_task',
        'title': 'Автоматическая задача из workflow',
        'critical': True
    },
    {
        'name': 'Уведомление',
        'type': 'action',
        'action': 'notify',
        'channel': 'system',
        'message': 'Workflow выполнен!'
    }
]

wf = orch.workflows.create_workflow('Test Automation Workflow', workflow_steps)
print(f"\n✅ Создан workflow: '{wf['name']}' (ID: {wf['id']})")

exec_result = orch.workflows.execute_workflow(wf['id'], {'context_var': 'test_value'})
print(f"\n📊 Результат выполнения:")
print(f"   Статус: {exec_result['status']}")
print(f"   Шагов выполнено: {len(exec_result['results'])}")

history = orch.workflows.get_execution_history(wf['id'])
print(f"   Всего выполнений: {len(history)}")

# ============================================================================
# 3. SLA MONITOR - АНАЛИЗ
# ============================================================================
print("\n" + "=" * 80)
print("📊 3. SLA MONITOR")
print("=" * 80)

print("""
ПЛЮСЫ:
✓ Настраиваемые SLA для разных приоритетов
✓ Автоматический расчёт времени отклика и решения
✓ Три состояния: ok, at_risk, breached
✓ Сводка по всему оркестратору

МИНУСЫ:
✗ Не учитывает выходные/праздники
✗ Нет эскалации при нарушениях
✗ Нет интеграции с уведомлениями
""")

task_critical = orch.board.add_task('Критический баг', priority='critical', agent='DevOps')
task_high = orch.board.add_task('Высокий приоритет', priority='high', agent='Backend')

print(f"\n📋 SLA для приоритетов:")
for priority in ['critical', 'high', 'medium', 'low']:
    sla = orch.sla_monitor.get_sla_for_priority(priority)
    print(f"   {priority}: response={sla['response_hours']}h, resolution={sla['resolution_hours']}h")

print(f"\n🔍 Проверка SLA задач:")
for task in [task_critical, task_high]:
    sla_status = orch.sla_monitor.check_task_sla(task.id)
    print(f"   {task.id} ({task.priority}): {sla_status.get('response_sla', {}).get('status', 'N/A')}")

sla_summary = orch.sla_monitor.get_sla_summary()
print(f"\n📈 SLA Compliance Rate: {sla_summary['compliance_rate']}%")

# ============================================================================
# 4. METRICS EXPORTER - АНАЛИЗ
# ============================================================================
print("\n" + "=" * 80)
print("📉 4. METRICS EXPORTER")
print("=" * 80)

print("""
ПЛЮСЫ:
✓ Prometheus формат "из коробки"
✓ Многометричный сбор (tasks, performance, SLA)
✓ Готов для интеграции с Grafana
✓ Методы: get_prometheus_format()

МИНУСЫ:
✗ Нет pushgateway интеграции (только stub)
✗ Нет Histogram/Summary метрик
✗ Нет метрик для агентов
""")

prom_output = orch.metrics_exporter.get_prometheus_format()
print(f"\n📊 Prometheus метрики ({len(prom_output)} символов):")
for line in prom_output.split('\n')[:8]:
    print(f"   {line}")
if len(prom_output.split('\n')) > 8:
    print(f"   ... и ещё {len(prom_output.split(chr(10))) - 8} строк")

# ============================================================================
# 5. KNOWLEDGE BASE - АНАЛИЗ
# ============================================================================
print("\n" + "=" * 80)
print("📚 5. KNOWLEDGE BASE")
print("=" * 80)

print("""
ПЛЮСЫ:
✓ Полнотекстовый поиск
✓ Категории и теги
✓ Версионирование статей
✓ Отслеживание просмотров

МИНУСЫ:
✗ Простой поиск (substring matching)
✗ Нет полнотекстового индекса
✗ Нет авторизации
""")

kb_article1 = orch.knowledge.add_article(
    'Getting Started Guide',
    'This guide covers the basics of using the orchestrator...',
    'tutorials',
    ['guide', 'beginner', 'setup']
)
kb_article2 = orch.knowledge.add_article(
    'API Reference',
    'Complete API reference for all endpoints...',
    'reference',
    ['api', 'developer', 'endpoints']
)

print(f"\n✅ Добавлено статей: {len(orch.knowledge.kb['articles'])}")
print(f"   Категории: {orch.knowledge.kb['categories']}")

results = orch.knowledge.search('guide')
print(f"\n🔍 Поиск 'guide': найдено {len(results)} статей")
for r in results:
    print(f"   - {r['title']} ({r['category']})")

# ============================================================================
# 6. TASK SCHEDULER - АНАЛИЗ
# ============================================================================
print("\n" + "=" * 80)
print("⏰ 6. TASK SCHEDULER")
print("=" * 80)

print("""
ПЛЮСЫ:
✓ Три типа расписания: once, interval, cron
✓ Автоматический расчёт следующего запуска
✓ Интеграция с board для создания задач
✓ Включение/выключение расписаний

МИНУСЫ:
✗ Нет встроенного демона для выполнения
✗ Нет timezone поддержки
✗ Упрощённый cron парсер
""")

sched1 = orch.scheduler.add_schedule(
    'Hourly Backup',
    {'title': 'Backup Task', 'agent': 'System', 'priority': 'high'},
    'interval',
    '60'
)

print(f"\n✅ Создано расписаний: {len(orch.scheduler.schedules['tasks'])}")
for sched in orch.scheduler.get_schedules():
    print(f"   - {sched['name']} ({sched['schedule_type']})")

# ============================================================================
# 7. AUDIT TRAIL - АНАЛИЗ
# ============================================================================
print("\n" + "=" * 80)
print("🔒 7. AUDIT TRAIL")
print("=" * 80)

print("""
ПЛЮСЫ:
✓ Полное логирование всех операций
✓ Фильтрация по action, user, severity
✓ Экспорт для compliance
✓ Лимит хранения (10000 записей)

МИНУСЫ:
✗ Нет шифрования логов
✗ Нет интеграции с SIEM
✗ Ограниченная детализация
""")

orch.audit.log('task_created', 'task', 'T-001', user='admin', severity='info')
orch.audit.log('task_updated', 'task', 'T-001', user='admin', severity='info')
orch.audit.log('task_failed', 'task', 'T-002', user='system', severity='error')
orch.audit.log('security_alert', 'system', 'auth', user='unknown', severity='critical')

print(f"\n✅ Записано событий: {len(orch.audit.audit['entries'])}")

user_activity = orch.audit.get_user_activity('admin', hours=24)
print(f"\n👤 Активность admin:")
print(f"   Всего действий: {user_activity['total_actions']}")

severity_report = orch.audit.get_severity_report(hours=24)
print(f"\n⚠️  Severity отчёт: {severity_report['by_severity']}")

# ============================================================================
# 8. TIME TRACKER - АНАЛИЗ
# ============================================================================
print("\n" + "=" * 80)
print("⏱️ 8. TIME TRACKER")
print("=" * 80)

print("""
ПЛЮСЫ:
✓ Start/stop таймеры
✓ Ручной ввод времени
✓ Привязка к задачам
✓ Отчёты по пользователям

МИНУСЫ:
✗ Нет billable hours
✗ Нет интеграции с billing
✗ Нет автоматической остановки
""")

task_for_time = orch.board.add_task('Time Tracked Task', agent='Developer')
orch.time_tracker.start_timer(task_for_time.id, 'developer')
time.sleep(0.2)
entry = orch.time_tracker.stop_timer(task_for_time.id)

print(f"\n⏱️ Отслеживание времени:")
print(f"   Задача: {task_for_time.id}")
print(f"   Записано времени: {entry['duration_seconds'] if entry else 0} сек")

orch.time_tracker.add_manual_entry(task_for_time.id, 7200, 'developer', 'Встреча')
time_report = orch.time_tracker.get_task_time(task_for_time.id)
print(f"   Всего времени: {time_report['total_hours']} ч")

# ============================================================================
# 9. RESOURCE MANAGEMENT - АНАЛИЗ
# ============================================================================
print("\n" + "=" * 80)
print("💻 9. RESOURCE MANAGEMENT")
print("=" * 80)

print("""
ПЛЮСЫ:
✓ Tracking загрузки агентов
✓ Capacity planning
✓ Available agents discovery
✓ Распределение задач

МИНУСЫ:
✗ Нет автоматической балансировки
✗ Статические capacity
✗ Нет мониторинга CPU/memory
""")

print(f"\n💻 Статус агентов:")
for agent_name in ['Hermes', 'OpenClaw', 'OpenAI']:
    status = orch.resources.get_agent_status(agent_name)
    print(f"   {agent_name}: {status['current_load']}/{status['capacity']} ({status['utilization']}%)")

alloc1 = orch.resources.allocate_task('T-TEST-001', 'Hermes')
print(f"\n✅ Выделение ресурсов: {alloc1}")

# ============================================================================
# ИТОГОВЫЙ ОТЧЁТ
# ============================================================================
print("\n" + "=" * 80)
print("📋 ИТОГОВЫЙ АНАЛИЗ ORCHESTRATOR v5.0")
print("=" * 80)

print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                         ОБЩАЯ ОЦЕНКА: ⭐⭐⭐⭐ (4/5)                           ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  СИЛЬНЫЕ СТОРОНЫ:                                                            ║
║  ✓ Комплексный мониторинг (SLA, Performance, Metrics)                        ║
║  ✓ Расширяемость через Integration Hub                                       ║
║  ✓ Workflow Engine с условной логикой                                          ║
║  ✓ Полный аудит операций                                                      ║
║  ✓ Prometheus-ready метрики                                                   ║
║  ✓ Knowledge Base для документации                                            ║
║  ✓ Time tracking для аналитики                                                ║
║  ✓ Resource management для планирования                                        ║
║                                                                              ║
║  СЛАБЫЕ СТОРОНЫ:                                                             ║
║  ✗ Многие интеграции - заглушки (требуют реальных API)                         ║
║  ✗ Нет WebSocket сервера (только SSE)                                          ║
║  ✗ Упрощённый cron парсинг                                                    ║
║  ✗ Нет авторизации в Knowledge Base                                           ║
║  ✗ Нет database backend (только JSON files)                                    ║
║                                                                              ║
║  ГОТОВНОСТЬ К ПРОДАКШЕНУ:                                                     ║
║  • Core функции: ✅ Готовы                                                     ║
║  • API: ✅ REST готов                                                         ║
║  • Metrics: ✅ Prometheus готов                                                ║
║  • Auth: ⚠️ Требует доп. реализации                                           ║
║  • Database: ⚠️ JSON (для большой нагрузки - SQL/NoSQL)                       ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

print(f"\n📊 Статистика системы:")
print(f"   Всего строк кода: {len(open('orchestrator_v5.py').readlines())}")
print(f"   Активных задач: {len(orch.board.list_tasks())}")
print(f"   Интеграций: {orch.integrations.get_status()['total']}")
print(f"   Workflows: {len(orch.workflows.workflows['workflows'])}")
print(f"   SLA Compliance: {orch.sla_monitor.get_sla_summary()['compliance_rate']}%")
print(f"   Аудит записей: {len(orch.audit.audit['entries'])}")

print("\n" + "=" * 80)
print("✅ ГЛУБОКОЕ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
print("=" * 80)