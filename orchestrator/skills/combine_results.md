# Skill: combine_results
## Назначение
Синтез и объединение результатов от нескольких агентов в единый структурированный отчёт.
Финальный шаг в большинстве multi-agent workflows.

## Когда использовать
- После параллельного выполнения Hermes + OpenClaw
- Нужен единый отчёт из нескольких источников данных
- Требуется сохранить итог в task board или knowledge base

## Входные параметры из context
| Переменная context | Источник | Описание |
|--------------------|----------|----------|
| `openclaw_result` | call_openclaw | Результаты внешнего поиска |
| `hermes_result` | call_hermes | Результаты внутреннего анализа |
| `camel_tasks` | call_camel | Список декомпозированных задач |
| `goal` | caller | Исходная цель пользователя |

## Команда (через Hermes)
**Команда:** `hermes --prompt "Combine and summarize: OPENCLAW={openclaw_result} HERMES={hermes_result}"`

## Пример вызова из WorkflowEngine
```json
{
  "name": "synthesize",
  "type": "action",
  "action": "call_hermes",
  "prompt": "Объедини результаты:\nВнешний поиск: {openclaw_result}\nВнутренний анализ: {hermes_result}\nСоздай итоговый структурированный отчёт."
}
```

## Последующие шаги (после combine)
1. `create_task` — сохранить итог в board
2. `notify` — уведомить пользователя о завершении
3. KnowledgeBase.add_article() — добавить в базу знаний

## Примечания
- Этот навык всегда выполняется последним в цепочке
- Не требует внешних вызовов — работает через context
- Результат сохраняется в `context["combined_result"]`
