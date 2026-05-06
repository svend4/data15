# Hybrid Orchestrator v5.0 — Полная документация

> **Тип системы:** Multi-Agent Orchestrator (Conductor/Mediator Pattern)  
> **Главный файл:** `orchestrator_v5.py` (6 579 строк)  
> **Архитектура:** B+A Hybrid — внутренние скиллы (B) + реальные коннекторы (A)  
> **Python:** 3.11+ | **Deps:** только stdlib (requests опционально для hermes_llm.py)

---

## Содержание

1. [Обзор архитектуры](#1-обзор-архитектуры)
2. [Структура файлов проекта](#2-структура-файлов-проекта)
3. [Быстрый старт](#3-быстрый-старт)
4. [CLI — все команды](#4-cli--все-команды)
5. [Классы orchestrator_v5.py](#5-классы-orchestrator_v5py)
6. [Workflows — сценарии автоматизации](#6-workflows--сценарии-автоматизации)
7. [Skills — скиллы агентов](#7-skills--скиллы-агентов)
8. [Task Templates — шаблоны задач](#8-task-templates--шаблоны-задач)
9. [REST API](#9-rest-api)
10. [Вспомогательные файлы](#10-вспомогательные-файлы)
11. [State-файлы (JSON)](#11-state-файлы-json)
12. [Конфигурация и переменные окружения](#12-конфигурация-и-переменные-окружения)
13. [Fallback-цепочки агентов](#13-fallback-цепочки-агентов)
14. [Архитектурные паттерны](#14-архитектурные-паттерны)

---

## 1. Обзор архитектуры

```
┌─────────────────────────────────────────────────────────┐
│                  HybridOrchestrator (v5)                │
│  Дирижёр: координирует 4 внешних агента + внутренние    │
│  компоненты через единый интерфейс CLI + REST API       │
└───────────────┬────────────────────────────────────────-┘
                │
    ┌───────────┼────────────────────────────┐
    ▼           ▼              ▼             ▼
┌──────┐  ┌─────────┐  ┌──────────┐  ┌────────┐
│CAMEL │  │ Hermes  │  │OpenClaw  │  │Multica │
│Layer │  │  LLM    │  │ Runner   │  │  API   │
│(стра-│  │(анализ) │  │(поиск)   │  │(доска) │
│тегия)│  └────┬────┘  └────┬─────┘  └───┬───-┘
└──────┘       │             │             │
         MiniMax API    openclaw CLI   localhost:3000
         fallback:      NVM+Node.js    fallback:
         hermes_llm.py  fallback:      BoardManager
                        CAMELLayer     (внутренняя)
```

### Три слоя

| Слой | Класс | Роль |
|------|-------|------|
| **Стратегический** | `CAMELLayer` | Декомпозиция целей на подзадачи |
| **Операционный** | `BoardManager` | Канбан-доска, статусы, персистентность |
| **Исполнительный** | `WorkflowEngine` | Выполнение шагов, вызов агентов |

### Принцип B+A Hybrid

- **B (Skills)** — `.md`-файлы в `orchestrator/skills/` описывают **КАК** вызывать агента
- **A (Connectors)** — методы в `WorkflowEngine` (`_call_hermes_agent`, `_call_openclaw_agent`, etc.) **ВЫПОЛНЯЮТ** вызов
- Скиллы — документация и шаблон команды. Коннекторы — реальный subprocess/HTTP.

---

## 2. Структура файлов проекта

```
data15/
├── orchestrator_v5.py          # ← ГЛАВНЫЙ ФАЙЛ (весь оркестратор)
├── hermes_llm.py               # MiniMax API интеграция для Hermes
├── hermes_integration.py       # Hermes как внутренний агент
├── hermes_agent_v2.py          # Расширенная версия Hermes агента
├── openclaw_integration.py     # OpenClaw интеграция
├── openclaw_runner.sh          # Shell-обёртка для вызова OpenClaw CLI
├── orchestrator_v4.py          # Предыдущая версия (архив)
├── orchestrator.py             # Первая версия (архив)
├── hybrid_orchestrator.py      # Прототип гибридной архитектуры
├── monitor_daemon.py           # Демон мониторинга в реальном времени
├── stress_test.py              # Нагрузочное тестирование
├── perf_test.py                # Тест производительности
├── deep_test.py                # Глубокое тестирование всех компонентов
├── test_full.py                # Полный набор тестов
├── test_hermes_complex.py      # Тесты Hermes
│
├── orchestrator/               # Рабочая директория оркестратора
│   ├── skills/                 # Скиллы агентов (md-документация)
│   │   ├── hermes_analyze.md
│   │   ├── openclaw_web_search.md
│   │   ├── camel_decompose.md
│   │   ├── multica_manage.md
│   │   └── combine_results.md
│   ├── state/                  # Состояние системы (JSON)
│   │   ├── workflows.json      # Определения workflow
│   │   ├── task_templates.json # Шаблоны задач
│   │   ├── config.json         # Конфигурация
│   │   ├── users.json          # Пользователи (RBAC)
│   │   ├── knowledge_base.json # База знаний
│   │   ├── integrations.json   # Настройки интеграций
│   │   ├── schedules.json      # Расписание задач
│   │   ├── cron_jobs.json      # Cron-задачи
│   │   ├── audit_trail.json    # Аудит лог
│   │   ├── hybrid_state.json   # Текущее состояние оркестратора
│   │   ├── webhooks.json       # Webhooks
│   │   ├── sla_config.json     # SLA настройки
│   │   └── resources.json      # Ресурсы агентов
│   ├── tasks/                  # Задачи и история
│   │   ├── hybrid_board.json   # ← Главная канбан-доска
│   │   ├── task_history.json   # История изменений
│   │   ├── task_comments.json  # Комментарии к задачам
│   │   ├── activity_feed.json  # Лента активности
│   │   └── backups/            # Автобэкапы доски
│   ├── cache/
│   │   └── results_cache.json  # Кэш результатов анализа
│   └── logs/                   # Логи выполнения агентов
│
└── docs/
    ├── DOCUMENTATION.md        # ← Этот файл
    └── PART2_PRODUCTION.md     # Документация Phase 2
```

---

## 3. Быстрый старт

```bash
# Показать все задачи
python3 orchestrator_v5.py /board

# Добавить задачу
python3 orchestrator_v5.py /add "Исследовать конкурентов"

# Запустить анализ через Hermes
python3 orchestrator_v5.py /analyze "Тренды AI в 2025"

# Запустить внешний поиск через OpenClaw
python3 orchestrator_v5.py /research "последние новости про AGI"

# Запустить оба агента параллельно
python3 orchestrator_v5.py /both "сравнение GPT-4 и Claude"

# Выполнить workflow
python3 orchestrator_v5.py /workflow run research_and_analyze "цель исследования"

# Запустить REST API сервер
python3 orchestrator_v5.py /api-server 5000

# Статус системы
python3 orchestrator_v5.py /status

# Полная справка
python3 orchestrator_v5.py /help
```

---

## 4. CLI — все команды

### Задачи

| Команда | Аргументы | Описание |
|---------|-----------|----------|
| `/board` | `[--all]` | Канбан-доска. `--all` показывает скрытые `__test__` задачи |
| `/add` | `<title>` | Создать задачу (поддерживает `ephemeral=true` через API) |
| `/search` | `<query> [--status s] [--agent a] [--priority p] [--tags t]` | Поиск задач по тексту и фильтрам |
| `/export` | `[json\|csv] [task_id...]` | Экспорт задач в файл |
| `/import` | `<file_path>` | Импорт задач из JSON |
| `/history` | `[task_id] [--action a] [--limit n]` | История изменений |
| `/comment-add` | `<task_id> <текст>` | Добавить комментарий к задаче |
| `/comment-list` | `<task_id>` | Список комментариев задачи |

### Агенты

| Команда | Аргументы | Описание |
|---------|-----------|----------|
| `/analyze` | `<topic>` | Анализ через Hermes. Результат кэшируется. Fallback: hermes_llm.py |
| `/research` | `<query>` | Внешний поиск через OpenClaw. Результат кэшируется. Fallback: заглушка |
| `/both` | `<topic>` | Hermes + OpenClaw **параллельно** (два потока), объединяет результаты |
| `/camel` | `<goal> [depth]` | Декомпозиция цели через CAMELLayer на подзадачи (depth=3 по умолчанию) |

### Workflows и Скиллы

| Команда | Аргументы | Описание |
|---------|-----------|----------|
| `/workflow list` | — | Список всех определённых workflows |
| `/workflow run` | `<id\|name> [goal]` | Выполнить workflow по ID или имени |
| `/skill list` | — | Список доступных скиллов (`.md` файлы в `skills/`) |
| `/skill info` | `<name>` | Показать документацию скилла |

### Шаблоны задач

| Команда | Аргументы | Описание |
|---------|-----------|----------|
| `/template-list` | — | Все шаблоны по категориям |
| `/template-create` | `<name> <title> [agent] [priority] [tags...]` | Создать шаблон |
| `/template-use` | `<template_name> [title_override]` | Создать задачу из шаблона |

### Мониторинг

| Команда | Аргументы | Описание |
|---------|-----------|----------|
| `/status` | — | Полный статус: агенты, доска, кэш, RBAC |
| `/health` | — | Проверка здоровья компонентов |
| `/metrics` | — | Метрики в формате Prometheus |
| `/stats` | — | Dashboard: статистика задач, агентов, SLA |
| `/events` | `[type] [limit]` | Последние события из EventBus |
| `/rate-limit` | — | Статус rate limiter |
| `/validate` | — | Валидация конфигурации |

### Конфигурация

| Команда | Аргументы | Описание |
|---------|-----------|----------|
| `/config` | — | Показать текущую конфигурацию |
| `/config cache-stats` | — | Статистика кэша (hits, misses, hit_rate) |
| `/config cache-clear` | — | Очистить кэш |

### Расписание

| Команда | Аргументы | Описание |
|---------|-----------|----------|
| `/cron-list` | — | Все cron-задачи |
| `/cron-add` | `<name> <command> <schedule>` | Добавить cron-задачу |

### RBAC (Пользователи)

| Команда | Аргументы | Описание |
|---------|-----------|----------|
| `/user-list` | — | Все пользователи с ролями |
| `/user-add` | `<username> <password> [role]` | Добавить пользователя (роли: admin/operator/viewer/guest) |

### Webhooks

| Команда | Аргументы | Описание |
|---------|-----------|----------|
| `/webhook-add` | `<url> [event1 event2 ...]` | Зарегистрировать webhook |
| `/webhook-list` | — | Все webhooks |
| `/webhook-remove` | `<webhook_id>` | Удалить webhook |

### Зависимости и очередь

| Команда | Аргументы | Описание |
|---------|-----------|----------|
| `/deps` | `[task_id]` | Граф зависимостей (DAG) |
| `/ready` | — | Задачи готовые к выполнению (зависимости закрыты) |
| `/queue` | — | Приоритетная очередь задач |
| `/dequeue` | — | Взять следующую задачу из очереди |
| `/timeline` | `[days]` | Временная шкала задач |
| `/gantt` | `[days]` | Gantt-данные для визуализации |

### Инфраструктура

| Команда | Аргументы | Описание |
|---------|-----------|----------|
| `/api-server` | `[port]` | Запустить REST API сервер (stdlib, без Flask) |
| `/executor-status` | — | Статус пула потоков AsyncTaskExecutor |
| `/backup` | — | Очистить старые бэкапы (оставить 10 последних) |
| `/help` | — | Полная справка |

---

## 5. Классы orchestrator_v5.py

### Инфраструктурные классы

---

#### `MetricsCollector` (L43–95)
**Prometheus-совместимый сборщик метрик.**

| Метод | Описание |
|-------|----------|
| `inc_counter(name, value=1)` | Увеличить счётчик (monotonic) |
| `set_gauge(name, value, labels)` | Установить gauge-значение |
| `observe_histogram(name, value)` | Записать значение в гистограмму |
| `get_metrics()` → `dict` | Все метрики как словарь |
| `export_prometheus()` → `str` | Текст в формате Prometheus text format |

**Используется:** создаётся один раз на уровне модуля (`metrics = MetricsCollector()`), хранится в `orch.metrics_collector`.

---

#### `CircuitBreaker` (L104–154)
**Защита от каскадных сбоев при вызовах внешних API.**

Состояния: `CLOSED` → `OPEN` → `HALF_OPEN` → `CLOSED`

| Метод | Описание |
|-------|----------|
| `call(func, *args, **kwargs)` | Выполнить функцию с защитой. При `OPEN` — выбрасывает `Exception` |
| `get_state()` → `dict` | Текущее состояние: state, failures, successes |

**Порог:** 5 ошибок подряд → `OPEN`. После 60 сек → `HALF_OPEN` (пробный вызов).

---

#### `RequestDeduplicator` (L160–207)
**Дедупликация одинаковых запросов в пределах временного окна.**

| Метод | Описание |
|-------|----------|
| `get_or_compute(key, compute_fn, *args)` | Вернуть кэшированный результат или вычислить |
| `get_stats()` → `dict` | hits, misses, saved_calls |
| `clear()` | Очистить кэш дедупликации |

**Окно:** 5 секунд (настраивается). Предотвращает повторные вызовы за `ttl` секунд.

---

#### `GracefulShutdownHandler` (L215–267)
**Обработка SIGTERM/SIGINT с сохранением данных.**

| Метод | Описание |
|-------|----------|
| `setup()` | Зарегистрировать обработчики сигналов |
| `shutdown()` | Инициировать graceful shutdown |
| `is_shutting_down()` → `bool` | Проверить флаг завершения |

---

#### `RateLimiter` (L302–334)
**Token bucket алгоритм. 614 820 ops/sec (измерено).**

| Метод | Описание |
|-------|----------|
| `is_allowed(key)` → `bool` | Проверить разрешение для ключа (IP/user/global) |
| `get_remaining(key)` → `int` | Оставшиеся токены |

---

#### `RetryHandler` (L337–361)
**Повторные попытки с exponential backoff.**

| Метод | Описание |
|-------|----------|
| `execute_with_retry(func, *args)` | Выполнить с retry. По умолчанию 3 попытки, backoff×2 |
| `get_attempts(key)` → `int` | Количество попыток для ключа |

---

#### `RBACManager` (L373–445)
**Role-Based Access Control. Пользователи: admin, operator, viewer, guest.**

| Метод | Описание |
|-------|----------|
| `authenticate(username, password)` → `dict\|None` | Аутентификация |
| `has_permission(username, action)` → `bool` | Проверка прав (read/write/admin/delete) |
| `add_user(username, password, role)` → `dict` | Создать пользователя |
| `list_users()` → `list` | Все пользователи |

**Роли и права:**

| Роль | read | write | delete | admin |
|------|------|-------|--------|-------|
| admin | ✅ | ✅ | ✅ | ✅ |
| operator | ✅ | ✅ | ✅ | ❌ |
| viewer | ✅ | ❌ | ❌ | ❌ |
| guest | ✅ | ❌ | ❌ | ❌ |

---

#### `EventBus` (L451–509)
**Pub/Sub шина событий. 177 755 ops/sec (измерено).**

| Метод | Описание |
|-------|----------|
| `subscribe(event_type, callback)` | Подписаться на тип события |
| `unsubscribe(event_type, callback)` | Отписаться |
| `publish(event_type, data)` | Опубликовать событие всем подписчикам |
| `get_events(event_type, limit)` → `list` | История последних событий |

**Основные типы событий:** `task.created`, `task.started`, `task.completed`, `task.failed`, `workflow.started`, `workflow.completed`, `cache.hit`

---

### Модели данных

---

#### `Task` (dataclass, L536–562)
**Основная сущность — задача на доске.**

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | `str` | Уникальный ID (формат `T-001`) |
| `title` | `str` | Заголовок задачи |
| `description` | `str` | Описание |
| `agent` | `str` | Назначенный агент: `Hermes`, `OpenClaw`, `CAMEL`, `Test` |
| `status` | `str` | `queued`, `running`, `completed`, `failed`, `blocked` |
| `priority` | `str` | `low`, `medium`, `high`, `critical` |
| `layer` | `str` | `execution`, `operational`, `strategic` |
| `complexity` | `int` | 1–10 |
| `tags` | `list[str]` | Теги. `__test__` → скрывается из `/board` |
| `ephemeral` | `bool` | **True** → автоматически удаляется при `completed`/`failed` |
| `retry_count` | `int` | Текущее число попыток |
| `max_retries` | `int` | Максимум попыток (default: 3) |
| `cached_result` | `str\|None` | Кэшированный результат |

**Специальный тег `__test__`:** задачи с этим тегом скрыты из `/board` (нужен `/board --all`).  
**Флаг `ephemeral=True`:** задача исчезает из доски сразу после завершения — не засоряет историю.

---

#### `TaskStatus` (Enum, L515)
`QUEUED`, `RUNNING`, `COMPLETED`, `FAILED`, `BLOCKED`

#### `Priority` (Enum, L525)
`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`

#### `CronJob` (dataclass, L731)
Поля: `id`, `name`, `command`, `schedule`, `enabled`, `last_run`, `next_run`, `run_count`

#### `OrchestratorState` (dataclass, L718)
Поля: `mode` (hybrid), `agents` (dict), `layers` (list)

---

### Хранилища данных

---

#### `CacheManager` (L867–953)
**Единый кэш: файловый persistence + TTL + статистика.**

| Метод | Описание |
|-------|----------|
| `get(key)` → `Any\|None` | Получить из кэша. `None` если истёк TTL или отсутствует |
| `set(key, value, ttl=None)` | Сохранить. TTL по умолчанию = `default_ttl` (3600 сек) |
| `invalidate(key)` → `bool` | Удалить конкретный ключ |
| `clear()` | Очистить весь кэш |
| `get_stats()` → `dict` | `entries`, `hits`, `misses`, `hit_rate`, `total_hits` |

**Формирование ключа:** используйте `orch._cache_key(prefix, data)` → `"prefix:md5hash[:12]"`.  
**Файл:** `orchestrator/cache/results_cache.json`

---

#### `BoardManager` (L1004–1248)
**Thread-safe канбан-доска с fcntl.flock и атомарными записями.**

| Метод | Описание |
|-------|----------|
| `add_task(title, **kwargs)` → `Task` | Создать задачу. kwargs: agent, priority, tags, description, layer, complexity, dependencies, ephemeral |
| `get_task(task_id)` → `Task\|None` | Получить задачу по ID |
| `update_status(task_id, status, progress)` → `bool` | Обновить статус. Если `ephemeral=True` и статус терминальный → **авто-удаление** |
| `update_task(task)` → `bool` | Сохранить изменённый объект Task |
| `list_tasks(status, show_test)` → `list[Task]` | Список задач. `show_test=False` скрывает `__test__` задачи |
| `get_task(task_id)` → `Task\|None` | Поиск по ID |
| `delete_task(task_id)` → `bool` | Удалить задачу |
| `get_stats()` → `dict` | total, queued, running, completed, failed |
| `increment_retry(task_id)` → `bool` | Увеличить retry_count |

**Особенности:**
- `fcntl.flock(LOCK_EX)` — исключительная блокировка на время записи
- `_atomic_save`: пишет во `.tmp`, затем `os.replace()` → атомарная замена
- Автобэкап при каждом сохранении в `tasks/backups/` (хранится 10 последних)
- `_gen_id()`: `T-{N:03d}` с учётом существующих ID

---

#### `TaskHistory` (L568–641)
**Аудит лог всех изменений задач.**

| Метод | Описание |
|-------|----------|
| `add_entry(task_id, action, data)` | Записать событие (created/updated/deleted/etc.) |
| `get_task_history(task_id)` → `list` | История конкретной задачи |
| `get_recent(limit)` → `list` | Последние N записей по всем задачам |
| `search(query, action, task_id, limit)` → `list` | Поиск в истории |

---

#### `TaskComments` (L647–714)
**Система комментариев к задачам.**

| Метод | Описание |
|-------|----------|
| `add_comment(task_id, content, author)` → `dict` | Добавить комментарий |
| `get_comments(task_id)` → `list` | Все комментарии задачи |
| `edit_comment(comment_id, content)` → `bool` | Редактировать |
| `delete_comment(comment_id)` → `bool` | Удалить |

---

#### `ConfigManager` (L752–864)
**Управление конфигурацией системы.**

| Метод | Описание |
|-------|----------|
| `get(key, default)` | Получить параметр конфигурации |
| `set(key, value)` | Установить параметр |
| `get_api_key()` → `str` | API ключ (из env или config) |
| `is_caching_enabled()` → `bool` | Кэширование включено? |
| `get_rate_limit()` → `dict` | max_requests, window_seconds |
| `get_retry_config()` → `dict` | max_retries, backoff_factor |
| `validate()` → `dict` | Проверка корректности конфигурации |

---

#### `CronJobManager` (L956–1001)
**Управление cron-задачами.**

| Метод | Описание |
|-------|----------|
| `add_job(name, command, schedule)` → `CronJob` | Добавить задание |
| `list_jobs()` → `list[CronJob]` | Все задания |
| `delete_job(job_id)` → `bool` | Удалить |
| `toggle_job(job_id)` → `bool` | Включить/выключить |

---

### Агентные классы

---

#### `CAMELLayer` (L1251–1304)
**Декомпозиция целей на подзадачи (внутренняя реализация без внешнего CAMEL).**

| Метод | Описание |
|-------|----------|
| `decompose(goal, depth)` → `list[dict]` | Разбить цель на задачи по уровням (depth=3) |
| `create_workflow(goal, depth)` → `list[Task]` | Декомпозировать и создать задачи на доске |

**Алгоритм fallback:** генерирует 3 фазы (Research/Analysis/Implementation) + до 9 подзадач.

---

#### `WorkflowEngine` (L5191–5537)
**Ядро автоматизации. Выполняет workflows, вызывает агентов.**

| Метод | Описание |
|-------|----------|
| `create_workflow(name, steps, trigger)` → `dict` | Создать новый workflow |
| `get_workflow(id)` → `dict\|None` | Найти по ID |
| `execute_workflow(id, context)` → `dict` | Выполнить workflow с контекстом |
| `_execute_step(step, context)` → `dict` | Выполнить один шаг |
| `_call_hermes_agent(prompt, context)` → `dict` | Вызвать Hermes: CLI → hermes_llm.py |
| `_call_openclaw_agent(prompt, context)` → `dict` | Вызвать OpenClaw: openclaw_runner.sh |
| `_call_multica_api(payload)` → `dict` | POST на Multica API → fallback BoardManager |
| `_call_camel_decompose(goal, depth)` → `dict` | CAMEL SDK → fallback CAMELLayer |
| `_run_skill(skill_name, context)` → `dict` | Прочитать скилл .md, выполнить команду |

**Типы шагов (`_execute_step`):**

| action | Что делает |
|--------|-----------|
| `create_task` | Создать задачу на доске |
| `update_status` | Обновить статус текущей задачи |
| `notify` | Отправить уведомление |
| `delay` | Пауза (секунды) |
| `call_hermes` | Вызов Hermes LLM агента |
| `call_openclaw` | Вызов OpenClaw агента |
| `call_multica` | Создать задачу в Multica |
| `call_camel` | CAMEL декомпозиция |
| `run_skill` | Выполнить скилл из skills/ |

---

### Главный класс

---

#### `HybridOrchestrator` (L1310–2242)
**Главный класс — точка входа для всех операций. 932 строки, 40 методов.**

**Атрибуты (инициализируются в `__init__`):**

| Атрибут | Тип | Описание |
|---------|-----|----------|
| `state` | `OrchestratorState` | Режим, агенты, слои |
| `board` | `BoardManager` | Канбан-доска |
| `config` | `ConfigManager` | Конфигурация |
| `cache` | `CacheManager` | Кэш результатов (файловый) |
| `events` | `EventBus` | Шина событий |
| `rate_limiter` | `RateLimiter` | Ограничение запросов |
| `retry_handler` | `RetryHandler` | Retry с backoff |
| `metrics_collector` | `MetricsCollector` | Сбор метрик |
| `circuit_breaker` | `CircuitBreaker` | Защита от каскадных сбоев |
| `rbac` | `RBACManager` | Управление доступом |
| `camel` | `CAMELLayer` | Декомпозиция целей |
| `history` | `TaskHistory` | История задач |
| `comments` | `TaskComments` | Комментарии |

**Атрибуты (lazy через @property/monkey-patching):**

| Атрибут | Класс | Описание |
|---------|-------|----------|
| `orch.dashboard` | `StatisticsDashboard` | Аналитика и отчёты |
| `orch.webhooks` | `WebhookManager` | HTTP webhooks |
| `orch.dependencies` | `TaskDependencyGraph` | DAG зависимостей |
| `orch.notifications` | `NotificationManager` | Уведомления |
| `orch.templates` | `TaskTemplatesLibrary` | Шаблоны задач |
| `orch.workflows` | `WorkflowEngine` | Движок workflows |
| `orch.knowledge` | `KnowledgeBase` | База знаний |
| `orch.health` | `HealthChecker` | Проверка здоровья |
| `orch.integrations` | `IntegrationHub` | Slack/Email/Webhook |
| `orch.scheduler` | `TaskScheduler` | Планировщик задач |
| `orch.metrics` | `MetricsExporter` | Экспорт метрик |
| `orch.metrics_exporter` | `MetricsExporter` | То же (alias) |
| `orch.audit` | `AuditTrail` | Audit trail |
| `orch.filters` | `TaskFilters` | Фильтрация задач |
| `orch.sla_monitor` | `SLAMonitor` | Мониторинг SLA |
| `orch.resources` | `ResourceManager` | Ресурсы агентов |
| `orch.time_tracker` | `TimeTracker` | Учёт времени |
| `orch.reports` | `ReportGenerator` | Генератор отчётов |
| `orch.extended_comments` | `ExtendedComments` | Расширенные комментарии |

> **Важно:** lazy-свойства создают **новый объект** при каждом обращении. Для состояния между вызовами используйте `board`, `cache`, `events` (они в `__init__`).

**Ключевые методы:**

| Метод | Описание |
|-------|----------|
| `_cache_key(prefix, data)` → `str` | Генерирует ключ: `"prefix:md5[:12]"` |
| `cmd_analyze(topic)` → `str` | Анализ через Hermes + кэш |
| `cmd_research(query)` → `str` | Поиск через OpenClaw + кэш |
| `cmd_both(topic)` → `str` | Параллельный запуск Hermes + OpenClaw |
| `cmd_camel(goal, depth)` → `str` | Декомпозиция через CAMEL |
| `cmd_workflow(action, id, goal)` → `str` | list / run workflows |
| `cmd_skill(action, name)` → `str` | list / info скиллов |
| `cmd_board(show_all)` → `str` | Отобразить доску |
| `cmd_status()` → `str` | Полный статус системы |
| `cmd_health()` → `str` | Проверка здоровья |
| `cmd_metrics()` → `str` | Prometheus метрики |
| `cmd_search(query, ...)` → `str` | Поиск задач |
| `cmd_help()` → `str` | Справка |

---

### Аналитические классы

---

#### `StatisticsDashboard` (L2741–2895)

| Метод | Описание |
|-------|----------|
| `get_summary()` → `dict` | tasks, cache, events, agents |
| `get_agent_performance()` → `dict` | Задачи и completion rate по агентам |
| `get_priority_distribution()` → `dict` | Распределение по приоритетам |
| `get_tag_cloud()` → `dict` | Частота тегов |
| `get_trends(days)` → `dict` | Тренды за период |
| `generate_report()` → `str` | Полный текстовый отчёт |

---

#### `TaskDependencyGraph` (L2902–3064)
**DAG (Directed Acyclic Graph) зависимостей между задачами.**

| Метод | Описание |
|-------|----------|
| `build_graph()` | Построить граф из текущей доски |
| `get_dependents(task_id)` → `list` | Задачи, зависящие от данной |
| `get_dependencies(task_id)` → `list` | Задачи, от которых зависит данная |
| `has_cycle()` → `bool` | Проверить на циклические зависимости |
| `topological_sort()` → `list` | Порядок выполнения |
| `get_ready_tasks()` → `list` | Задачи с выполненными зависимостями |
| `visualize(task_id)` → `str` | ASCII-дерево зависимостей |

---

#### `AsyncTaskExecutor` (L3071–3141)
**Пул потоков для фоновых задач.**

| Метод | Описание |
|-------|----------|
| `submit(task_id, func, *args)` → `str` | Запустить в фоне, вернуть future_id |
| `get_result(future_id)` → `Any` | Получить результат (блокирующий) |
| `is_running(task_id)` → `bool` | Выполняется ли задача |
| `cancel(future_id)` → `bool` | Отменить |
| `get_status()` → `dict` | max_workers, running, completed, tasks |
| `shutdown(wait)` | Остановить пул |

---

#### `PriorityTaskQueue` (L3245–3338)
**Приоритетная очередь с поддержкой critical/high/medium/low.**

| Метод | Описание |
|-------|----------|
| `enqueue(task_id, priority)` | Добавить в очередь |
| `dequeue()` → `str\|None` | Взять задачу с наивысшим приоритетом |
| `peek()` → `str\|None` | Посмотреть следующую без удаления |
| `get_queue(limit)` → `list` | N первых задач с позициями |
| `get_stats()` → `dict` | total_queued, by_priority, next_task |

---

#### `TaskTimeline` (L3148–3236)

| Метод | Описание |
|-------|----------|
| `get_timeline(days)` → `list` | События за период |
| `get_gantt_data(days)` → `list` | Данные для Gantt (id, title, start, end, progress) |
| `generate_calendar_view(days)` → `str` | ASCII-календарь |

---

### Системные классы

---

#### `NotificationManager` (L3345–3439)

| Метод | Описание |
|-------|----------|
| `notify(title, message, level)` → `dict` | Создать уведомление (info/warning/error/success) |
| `send(channel, message, level)` | Alias для `notify()`, используется WorkflowEngine |
| `get_notifications(unread_only, limit)` → `list` | Получить уведомления |
| `mark_read(notif_id)` → `bool` | Отметить прочитанным |
| `mark_all_read()` | Отметить все прочитанными |
| `get_unread_count()` → `int` | Количество непрочитанных |

---

#### `WebhookManager` (L3647–3730)

| Метод | Описание |
|-------|----------|
| `add_webhook(url, events)` → `dict` | Зарегистрировать webhook |
| `remove_webhook(webhook_id)` → `bool` | Удалить |
| `list_webhooks()` → `list` | Все webhooks |
| `trigger(event_type, data)` | Отправить событие всем подходящим webhooks (POST JSON) |

---

#### `IntegrationHub` (L5825–5980)
**Интеграция с внешними сервисами: Slack, Email, Webhook.**

| Метод | Описание |
|-------|----------|
| `register_integration(name, type, config)` → `dict` | Зарегистрировать интеграцию |
| `enable_integration(name)` / `disable_integration(name)` | Вкл/выкл |
| `send_notification(integration_name, message, data)` → `dict` | Отправить |
| `_send_slack(config, message, data)` → `dict` | POST на Slack webhook URL с `{"text": msg}` |
| `_send_email(config, message, data)` → `dict` | SMTP (starttls) или fallback в лог-файл |
| `_send_webhook(config, message, data)` → `dict` | Произвольный HTTP POST |

**Конфигурация Slack:**
```json
{"webhook_url": "https://hooks.slack.com/..."}
```
**Конфигурация Email:**
```json
{"smtp_host": "smtp.gmail.com", "smtp_port": 587, "smtp_user": "...", "smtp_password": "...", "to": "...", "from": "..."}
```

---

#### `TaskScheduler` (L5987–6152)
**Планировщик задач с cron-выражениями.**

| Метод | Описание |
|-------|----------|
| `add_schedule(name, template, type, value)` → `dict` | Добавить расписание. type: once/interval/cron |
| `get_due_tasks()` → `list` | Задачи, время которых пришло |
| `execute_scheduled_task(task_id)` → `dict` | Создать задачу по расписанию |
| `get_schedules(enabled_only)` → `list` | Все расписания |
| `delete_schedule(task_id)` → `bool` | Удалить |
| `start_background_loop(interval=60)` → `Thread` | **Запустить daemon-поток** — автоматически запускается при `/api-server` |

**Типы расписаний:**
- `once` — одноразовый запуск (ISO datetime)
- `interval` — каждые N минут
- `cron` — cron-выражение (упрощённое: `minute hour day_of_month month day_of_week`)

---

#### `TaskFilters` (L3446–3522)

| Метод | Описание |
|-------|----------|
| `filter_tasks(tasks, query, status, agent, tags, priority)` → `list` | Мультикритериальная фильтрация |
| `get_facets()` → `dict` | Доступные значения для каждого фильтра |
| `save_filter(name, criteria)` → `dict` | Сохранить фильтр для повторного использования |
| `get_saved_filters()` → `list` | Сохранённые фильтры |

---

#### `AutoAssignRules` (L3529–3594)
**Автоматическое назначение агентов по правилам.**

| Метод | Описание |
|-------|----------|
| `add_rule(pattern, agent, priority_filter, tag_filter)` → `dict` | Добавить правило |
| `match_task(task)` → `str\|None` | Найти подходящего агента для задачи |
| `get_stats()` → `dict` | Статистика правил |

---

#### `SLAMonitor` (L4709–4838)

| Метод | Описание |
|-------|----------|
| `get_sla_for_priority(priority)` → `dict` | SLA для приоритета (часы) |
| `check_task_sla(task)` → `dict` | Статус SLA конкретной задачи |
| `get_sla_violations()` → `list` | Все нарушения SLA |
| `get_sla_summary()` → `dict` | Сводка: compliance rate, violations |

**SLA по умолчанию:**
| Приоритет | SLA |
|-----------|-----|
| critical | 4 часа |
| high | 24 часа |
| medium | 72 часа |
| low | 168 часов (7 дней) |

---

#### `ResourceManager` (L4845–4960)
**Управление ёмкостью агентов.**

| Метод | Описание |
|-------|----------|
| `get_agent_status(agent)` → `dict` | Загрузка агента |
| `allocate_task(task_id, agent)` → `bool` | Выделить ресурс |
| `release_task(task_id, agent)` → `bool` | Освободить ресурс |
| `get_available_agents()` → `list` | Агенты с свободными слотами |
| `set_agent_capacity(agent, max_tasks)` | Установить лимит |
| `get_allocation_summary()` → `dict` | Сводка по всем агентам |

---

#### `AuditTrail` (L4967–5078)
**Полный аудит всех операций.**

| Метод | Описание |
|-------|----------|
| `log(user, action, entity_type, entity_id, data, severity)` | Записать событие |
| `query(user, action, entity_type, severity, limit)` → `list` | Поиск по параметрам |
| `get_entity_history(entity_id)` → `list` | История сущности |
| `get_user_activity(user)` → `list` | Активность пользователя |
| `export_audit_log(format)` → `str` | Экспорт в JSON/CSV |

---

#### `KnowledgeBase` (L5544–5644)

| Метод | Описание |
|-------|----------|
| `add_article(title, content, category, tags)` → `dict` | Добавить статью |
| `get_article(article_id)` → `dict\|None` | Получить по ID |
| `search(query, category)` → `list` | Полнотекстовый поиск |
| `update_article(id, updates)` → `bool` | Обновить |
| `get_by_category(category)` → `list` | По категории |
| `get_popular(limit)` → `list` | Популярные (по view_count) |

**Статьи по умолчанию:** Hermes CLI, OpenClaw CLI, Multica API, CAMEL-AI SDK, WorkflowEngine actions, B+A Hybrid Architecture.

---

#### `HealthChecker` (L5719–5818)

| Метод | Описание |
|-------|----------|
| `register_check(name, check_fn)` | Зарегистрировать проверку |
| `run_checks()` → `dict` | Выполнить все проверки, вернуть результаты |
| `get_readiness()` → `bool` | Готов ли к обработке запросов |
| `get_liveness()` → `bool` | Жив ли процесс |

**Встроенные проверки:** board (файл доступен), state (JSON валиден), tasks (доска читается), memory (heap < 500MB).

---

#### `MetricsExporter` (L6159–6243)

| Метод | Описание |
|-------|----------|
| `collect_metrics()` → `dict` | Собрать все метрики системы |
| `export_prometheus()` → `str` | Формат Prometheus text |
| `export_json()` → `str` | JSON |
| `export_csv()` → `str` | CSV |
| `get_prometheus_format()` → `str` | Alias для `export_prometheus()` |
| `push_to_gateway(url)` | Отправить на Pushgateway |

---

#### `WebSocketManager` (L5085–5184)
**Server-Sent Events для real-time стриминга.**

| Метод | Описание |
|-------|----------|
| `subscribe(client_id, event_types)` | Подписать клиента |
| `publish(event_type, data)` | Опубликовать событие |
| `broadcast_task_update(task)` | Обновление задачи всем подписчикам |
| `broadcast_metrics(metrics)` | Метрики в реальном времени |
| `broadcast_alert(message, severity)` | Алерт |

---

#### `TimeTracker` (L4555–4702)

| Метод | Описание |
|-------|----------|
| `start_timer(task_id, user)` → `str` | Начать отсчёт, вернуть timer_id |
| `stop_timer(timer_id)` → `dict` | Остановить, вернуть elapsed |
| `add_manual_entry(task_id, user, minutes, description)` → `dict` | Ручной ввод |
| `get_task_time(task_id)` → `dict` | Общее время на задачу |
| `get_active_timers()` → `list` | Активные таймеры |
| `get_user_time_report(user, days)` → `dict` | Отчёт по пользователю |

---

#### `TaskTemplatesLibrary` (L4318–4548)

| Метод | Описание |
|-------|----------|
| `add_template(name, category, fields, defaults, description)` → `dict` | Создать шаблон |
| `get_template(id)` → `dict\|None` | По ID |
| `list_templates(category)` → `list` | Все или по категории |
| `create_task_from_template(template_id, overrides)` → `Task` | Создать задачу |
| `delete_template(id)` → `bool` | Удалить |
| `get_categories()` → `list` | Список категорий |

---

#### `PerformanceMonitor` (L4220–4311)

| Метод | Описание |
|-------|----------|
| `record_operation(operation, duration, success)` | Записать замер |
| `get_operation_stats(operation)` → `dict` | avg/min/max/p95/p99 latency |
| `get_all_stats()` → `dict` | По всем операциям |
| `get_recent_errors(limit)` → `list` | Последние ошибки |
| `get_summary()` → `dict` | Сводка |

---

#### `RecurringTaskManager` (L3737–3902)

| Метод | Описание |
|-------|----------|
| `add_recurring_task(name, template, interval_minutes, enabled)` → `dict` | Добавить повторяющуюся задачу |
| `get_due_tasks()` → `list` | Задачи, которые нужно выполнить |
| `execute_task(task_id)` → `dict` | Выполнить (создать на доске) |

---

#### `DataMigration` (L4144–4213)

| Метод | Описание |
|-------|----------|
| `export_all()` → `dict` | Экспорт всех данных системы |
| `import_all(data)` | Импорт из словаря |
| `backup_to_file(path)` | Сохранить backup в файл |
| `restore_from_file(path)` | Восстановить из файла |

---

#### `BatchOperations` (L4007–4141)

| Метод | Описание |
|-------|----------|
| `bulk_update_status(task_ids, status)` → `dict` | Массовое обновление статуса |
| `bulk_update_priority(task_ids, priority)` → `dict` | Массовое изменение приоритета |
| `bulk_add_tags(task_ids, tags)` → `dict` | Массовое добавление тегов |
| `bulk_delete(task_ids)` → `dict` | Массовое удаление |
| `bulk_assign(task_ids, agent)` → `dict` | Переназначить агента |
| `get_batch_stats()` → `dict` | Статистика batch-операций |

---

### REST API класс

---

#### `RestAPI` (L2569–2728)
**HTTP сервер на stdlib `http.server`. Без Flask, без pip.**

**Запуск:**
```bash
python3 orchestrator_v5.py /api-server 5000
# REST API started on http://0.0.0.0:5000/api/health
```

---

## 6. Workflows — сценарии автоматизации

Файл: `orchestrator/state/workflows.json`  
Выполнение: `python3 orchestrator_v5.py /workflow run <id|name> [goal]`

---

### [1] Test Workflow
**Базовый тест.** `create_task` → `delay(1s)` → `update_status(completed)`

---

### [2] research_and_analyze
**OpenClaw исследует → Hermes анализирует → задача создана → уведомление.**

```
external_search  → call_openclaw  (поиск по {goal})
internal_analysis → call_hermes   (анализ данных openclaw)
save_result       → create_task   (сохранить результат)
notify_done       → notify        (уведомление)
```

**Переменные контекста:** `{goal}`, `{openclaw_result}` (авто)

```bash
python3 orchestrator_v5.py /workflow run research_and_analyze "тренды AI 2025"
```

---

### [3] decompose_and_execute
**CAMEL декомпозирует → Multica создаёт задачи → OpenClaw исследует → Hermes анализирует.**

```
decompose_goal       → call_camel    (разбить цель)
create_in_multica    → call_multica  (создать на доске)
execute_research     → call_openclaw (исследование)
execute_analysis     → call_hermes   (анализ)
finalize             → notify
```

---

### [4] monitor_and_report
**Мониторинг системы и отчёт.**

```
health_check    → create_task  (задача мониторинга)
run_hermes_check → call_hermes (Hermes анализирует состояние)
update_done     → update_status(completed)
notify_report   → notify       ({hermes_result} в сообщение)
```

```bash
python3 orchestrator_v5.py /workflow run monitor_and_report
```

---

### [5] full_pipeline
**Полный пайплайн: все 4 агента по очереди.**

```
decompose     → call_camel              (CAMEL: разбивка цели)
research      → run_skill(openclaw_web_search)
analyze       → call_hermes             (анализ всего)
combine       → run_skill(combine_results)
persist_multica → call_multica          (сохранить итог)
done_notify   → notify
```

**Самый мощный workflow. Использует все 4 агента.**

```bash
python3 orchestrator_v5.py /workflow run full_pipeline "создать AI-продукт для b2b"
```

---

### Создание кастомного workflow (программно)

```python
from orchestrator_v5 import HybridOrchestrator, WorkflowEngine

orch = HybridOrchestrator()
wf = WorkflowEngine(orch)

my_workflow = wf.create_workflow("my_flow", steps=[
    {"name": "step1", "type": "action", "action": "call_hermes",
     "prompt": "Analyze: {goal}", "critical": True},
    {"name": "step2", "type": "action", "action": "notify",
     "channel": "my_channel", "message": "Done: {hermes_result}"},
])
result = wf.execute_workflow(my_workflow["id"], {"goal": "исследование"})
```

---

## 7. Skills — скиллы агентов

Файл-хранилище: `orchestrator/skills/*.md`  
Просмотр: `python3 orchestrator_v5.py /skill info <name>`

---

### hermes_analyze
**Когда:** нужен аналитический отчёт, синтез данных, генерация кода.

```bash
# Реальный вызов
hermes --prompt "{goal}" --output json

# MiniMax fallback
python3 hermes_llm.py "{goal}"
```

**Входные переменные контекста:** `{goal}`, `{openclaw_result}`

---

### openclaw_web_search
**Когда:** нужна актуальная информация из сети, новости, данные API.

```bash
bash openclaw_runner.sh --prompt "Search: {goal}" --timeout 120
```

**Требования:** Node.js v22+ через NVM, OpenClaw CLI установлен.

---

### camel_decompose
**Когда:** сложная цель требует разбивки на конкретные шаги.

```python
# CAMEL-AI SDK
from camel.agents import TaskPlannerAgent
planner = TaskPlannerAgent()
tasks = planner.plan(goal="{goal}", depth=3)
```

**Fallback:** внутренний `CAMELLayer.decompose()`

---

### multica_manage
**Когда:** нужно создать задачу в Multica kanban-доске.

```bash
curl -X POST http://localhost:3000/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"title":"{title}","agent":"{agent}"}'
```

**Fallback:** `BoardManager.add_task()` (внутренняя доска)

---

### combine_results
**Когда:** финальный шаг — объединить результаты нескольких агентов.

```
hermes --prompt "Combine: OPENCLAW={openclaw_result} HERMES={hermes_result}"
```

**Входные переменные:** `{openclaw_result}`, `{hermes_result}`, `{camel_tasks}`, `{goal}`

---

## 8. Task Templates — шаблоны задач

Файл: `orchestrator/state/task_templates.json`  
Команды: `/template-list`, `/template-use <name>`, `/template-create <name> ...`

### Встроенные шаблоны

| ID | Имя | Категория | Поля | Агент |
|----|-----|-----------|------|-------|
| 1 | Bug Report | issues | title, severity, steps, expected, actual | OpenClaw |
| 2 | Feature Request | features | title, user_story, acceptance, priority, effort | Hermes |
| 3 | Code Review | development | title, pr_url, reviewer, changes_summary | OpenClaw |
| 4 | **Hermes Analysis** | **agents** | topic, depth, output_format | **Hermes** |
| 5 | **OpenClaw Search** | **agents** | query, sources, timeout | **OpenClaw** |
| 6 | **CAMEL Decompose** | **agents** | goal, depth, assign_to | **CAMEL** |
| 7 | **Full Pipeline** | **agents** | goal, workflow_id | **Hermes** |
| 8 | **Stress Test** | **testing** | title, concurrency, iterations, agent | **Test** |

### Шаблон Stress Test (специальный)
- `ephemeral: true` → задача **автоматически удаляется** после завершения
- Тег `__test__` → скрывается из `/board` (виден только в `/board --all`)
- Используется для нагрузочного тестирования без засорения доски

### Создание задачи из шаблона

```bash
python3 orchestrator_v5.py /template-use "Hermes Analysis"
python3 orchestrator_v5.py /template-use "Bug Report" "Crash on login page"
```

---

## 9. REST API

**Запуск:** `python3 orchestrator_v5.py /api-server [port]`  
**Base URL:** `http://localhost:5000`  
**Формат:** JSON (Content-Type: application/json)  
**Зависимости:** только Python stdlib (http.server, urllib.parse)

### Эндпоинты

#### Служебные

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/health` | `{"status": "ok", "version": "5.0", "timestamp": "..."}` |
| GET | `/api/status` | mode, stats, agents |
| GET | `/api/metrics` | Prometheus text format |
| GET | `/api/stats` | board stats, cache stats, circuit breaker |
| GET | `/api/events` | `?type=task.completed&limit=50` |
| GET | `/api/workflows` | Список workflow определений |

#### Задачи

| Метод | Путь | Тело / Параметры | Описание |
|-------|------|------------------|----------|
| GET | `/api/tasks` | `?status=queued` | Список задач |
| POST | `/api/tasks` | `{title, agent, priority, tags, description, ephemeral}` | Создать задачу |
| GET | `/api/tasks/{id}` | — | Получить задачу |
| PATCH/PUT | `/api/tasks/{id}` | `{status, priority, progress}` | Обновить задачу |
| DELETE | `/api/tasks/{id}` | — | Удалить задачу |

#### История и комментарии

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/tasks/{id}/history` | История задачи |
| GET | `/api/history` | `?limit=50` Последние события |
| POST | `/api/tasks/{id}/comments` | `{content, author}` Добавить комментарий |

#### Поиск

| Метод | Путь | Параметры | Описание |
|-------|------|-----------|----------|
| GET | `/api/search` | `?q=текст&status=queued&agent=Hermes&priority=high&tags=bug` | Поиск задач |

#### Workflows

| Метод | Путь | Тело | Описание |
|-------|------|------|----------|
| POST | `/api/workflows/{id}/run` | `{"goal": "..."}` | Запустить workflow |

### Примеры

```bash
# Создать задачу
curl -X POST http://localhost:5000/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"title": "Анализ конкурентов", "agent": "Hermes", "priority": "high"}'

# Ephemeral задача (удалится после завершения)
curl -X POST http://localhost:5000/api/tasks \
  -d '{"title": "Stress test", "ephemeral": true, "agent": "Test"}'

# Обновить статус
curl -X PATCH http://localhost:5000/api/tasks/T-001 \
  -d '{"status": "completed", "progress": 100}'

# Запустить workflow
curl -X POST http://localhost:5000/api/workflows/2/run \
  -d '{"goal": "исследовать рынок AI инструментов"}'

# Метрики
curl http://localhost:5000/api/metrics
```

---

## 10. Вспомогательные файлы

### `hermes_llm.py`
**Интеграция Hermes с MiniMax API.**

| Класс/функция | Описание |
|---------------|----------|
| `HermesLLM` | Основной класс. `api_key` из `MINIMAX_API_KEY` env |
| `HermesLLM.analyze(task_id, topic, context)` → `dict` | Анализ через MiniMax API. Fallback: встроенный текст |
| `HermesLLM._find_working_endpoint()` | Пробует 3 endpoint'а MiniMax |
| `get_hermes_llm()` | Глобальный singleton |

**CLI-режим (для WorkflowEngine fallback):**
```bash
python3 hermes_llm.py "Тема для анализа"
# → выводит текст анализа в stdout
```

**Переменная окружения:** `MINIMAX_API_KEY`

---

### `openclaw_runner.sh`
**Shell-обёртка для запуска OpenClaw CLI.**

```bash
./openclaw_runner.sh --prompt "поиск запроса" [--output file] [--timeout 120]
```

**Что делает:**
1. Ищет Node.js через NVM (несколько путей)
2. Устанавливает `PATH` для openclaw
3. Запускает: `openclaw agent --local --message "$PROMPT" --session-id "$SESSION_ID"`
4. При ошибке выводит `[openclaw_runner] ERROR: ...` в stderr

**Fallback-пути для Node.js:**
- `$HOME/.nvm/versions/node/v22.22.2/bin`
- `$HOME/.nvm/versions/node/v22.12.0/bin`
- `/tmp/node-v22.12.0-linux-x64/bin`
- `/home/minimax/.nvm/versions/node/v22.22.2/bin`

---

### `hermes_integration.py`
**Hermes как внутренний агент** (без MiniMax API — использует встроенные возможности).

| Функция | Описание |
|---------|----------|
| `analyze(topic, context)` | Анализ встроенными средствами |
| `generate_code(spec)` | Генерация кода |
| `review_code(code)` | Ревью кода |
| `save_memory(key, data)` | Сохранить в память |
| `recall_memory(key)` | Вспомнить из памяти |

---

### `openclaw_integration.py`
**OpenClaw как внешний агент** (поиск в интернете, выполнение задач).

| Функция | Описание |
|---------|----------|
| `search(query, sources)` | Поиск в сети |
| `execute_task(task_description)` | Выполнить задачу |
| `get_status()` | Статус агента |

---

### `monitor_daemon.py`
**Демон мониторинга в реальном времени.**

```bash
python3 monitor_daemon.py
```

Отслеживает изменения `hybrid_board.json`, логирует статусы, генерирует алерты при сбоях.

---

### `stress_test.py` / `perf_test.py` / `deep_test.py`
**Тестовые файлы.** Все задачи создаются с `ephemeral=True` (после обновления) чтобы не засорять доску.

---

## 11. State-файлы (JSON)

| Файл | Назначение | Класс |
|------|-----------|-------|
| `tasks/hybrid_board.json` | **Главная доска задач** | `BoardManager` |
| `state/workflows.json` | Определения workflows | `WorkflowEngine` |
| `state/task_templates.json` | Шаблоны задач | `TaskTemplatesLibrary` |
| `state/config.json` | Конфигурация | `ConfigManager` |
| `state/users.json` | Пользователи RBAC | `RBACManager` |
| `state/knowledge_base.json` | База знаний | `KnowledgeBase` |
| `state/integrations.json` | Настройки интеграций | `IntegrationHub` |
| `state/schedules.json` | Расписание задач | `TaskScheduler` |
| `state/cron_jobs.json` | Cron-задачи | `CronJobManager` |
| `state/audit_trail.json` | Аудит лог | `AuditTrail` |
| `state/hybrid_state.json` | Текущий статус оркестратора | `OrchestratorState` |
| `state/webhooks.json` | Webhooks | `WebhookManager` |
| `state/sla_config.json` | SLA конфигурация | `SLAMonitor` |
| `state/resources.json` | Ресурсы агентов | `ResourceManager` |
| `state/performance_metrics.json` | Метрики производительности | `PerformanceMonitor` |
| `state/recurring_tasks.json` | Повторяющиеся задачи | `RecurringTaskManager` |
| `tasks/task_history.json` | История изменений | `TaskHistory` |
| `tasks/task_comments.json` | Комментарии | `TaskComments` |
| `tasks/activity_feed.json` | Лента активности | `ActivityFeed` |
| `tasks/notifications.json` | Уведомления | `NotificationManager` |
| `tasks/time_tracking.json` | Учёт времени | `TimeTracker` |
| `cache/results_cache.json` | Кэш результатов | `CacheManager` |

### Формат `hybrid_board.json`

```json
{
  "version": "5.0",
  "stats": {"total": 21, "queued": 15, "running": 0, "completed": 4, "failed": 2},
  "tasks": [
    {
      "id": "T-001",
      "title": "Название задачи",
      "agent": "Hermes",
      "status": "queued",
      "priority": "medium",
      "tags": [],
      "ephemeral": false,
      "layer": "execution",
      "complexity": 5,
      "retry_count": 0,
      "max_retries": 3
    }
  ]
}
```

---

## 12. Конфигурация и переменные окружения

### Переменные окружения

| Переменная | Описание | По умолчанию |
|-----------|----------|-------------|
| `MINIMAX_API_KEY` | API ключ MiniMax для Hermes | (пусто — fallback) |
| `OPENCLAW_NO_TELEMETRY` | Отключить телеметрию OpenClaw | `1` (в runner) |

### `orchestrator/state/config.json`

```json
{
  "cache_enabled": true,
  "cache_ttl": 3600,
  "rate_limit": {"max_requests": 100, "window_seconds": 60},
  "retry": {"max_retries": 3, "backoff_factor": 2}
}
```

### Программная настройка

```python
from orchestrator_v5 import HybridOrchestrator

orch = HybridOrchestrator()
orch.config.set("cache_ttl", 7200)          # кэш 2 часа
orch.config.set("cache_enabled", False)     # отключить кэш
orch.config.set_api_key("sk-...", "minimax") # установить API ключ
```

---

## 13. Fallback-цепочки агентов

### Hermes
```
1. hermes CLI           → which hermes → hermes --prompt "..." --output json
2. hermes_llm.py CLI    → python3 hermes_llm.py "..."
3. MiniMax API          → POST https://api.minimax.chat/v1/chat/completions
4. Встроенный текст     → шаблон с placeholder'ами
```

### OpenClaw
```
1. openclaw_runner.sh   → bash openclaw_runner.sh --prompt "..."
   внутри:
   1a. NVM Node.js v22  → openclaw agent --local --message "..."
   1b. Fallback paths   → несколько путей NVM
2. Текст-заглушка       → "[openclaw_runner] ERROR: ..." + пустой результат
```

### Multica
```
1. HTTP POST            → http://localhost:3000/api/tasks
2. BoardManager         → add_task() на внутреннюю доску
```

### CAMEL
```
1. camel-ai SDK         → import camel; TaskPlannerAgent()
2. CAMELLayer           → внутренний алгоритм декомпозиции
```

### Кэш
```
1. In-memory hit        → time.time() < entry["expires"]
2. Файловый hit         → results_cache.json
3. Промах               → вызов агента → сохранение в кэш
```

---

## 14. Архитектурные паттерны

### Orchestrator Pattern
`HybridOrchestrator` — центральный координатор. Все агенты обращаются через него, не напрямую друг к другу.

### Adapter Pattern
`WorkflowEngine._call_*_agent()` — адаптеры, преобразующие единый интерфейс `{prompt, context}` → `{output, success}` для каждого агента с разным протоколом (CLI, HTTP, SDK).

### Mediator Pattern
`EventBus` — все события проходят через шину, компоненты не знают друг о друге.

### Circuit Breaker Pattern
`CircuitBreaker` защищает внешние вызовы. После 5 ошибок — `OPEN` (60 сек паузы).

### Strategy Pattern
Скиллы (`.md` файлы) — стратегии вызова агентов. `_run_skill()` читает стратегию и выполняет.

### Template Method Pattern
`WorkflowEngine.execute_workflow()` — шаблонный метод. Конкретные шаги определяются в JSON.

### Ephemeral Object Pattern (кастомный)
Задачи с `ephemeral=True` автоматически удаляются при завершении — «одноразовые объекты».

---

## Быстрая справка: что где менять

| Хочу... | Где менять |
|---------|-----------|
| Добавить новую CLI команду | `HybridOrchestrator.cmd_*()` + routing в `main()` |
| Добавить тип шага в workflow | `WorkflowEngine._execute_step()` |
| Добавить нового агента | `WorkflowEngine._call_*_agent()` + skill .md |
| Добавить шаблон задачи | `task_templates.json` + `TaskTemplatesLibrary._create_defaults()` |
| Добавить REST endpoint | `RestAPI.Handler.do_GET/POST/PATCH` |
| Изменить SLA | `orchestrator/state/sla_config.json` |
| Настроить Slack | `integrations.json` → webhook_url |
| Добавить webhook | `/webhook-add https://... event_type` |
| Изменить кэш TTL | `config.json` → cache_ttl |
| Добавить пользователя | `/user-add username password role` |
| Создать workflow | `workflows.json` или `WorkflowEngine.create_workflow()` |

---

*Документация сгенерирована для Hybrid Orchestrator v5.0*  
*Ветка: `claude/review-project-status-LNsSj`*
