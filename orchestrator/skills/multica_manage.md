# Skill: multica_manage
## Назначение
Создание и управление задачами в Multica через REST API.
При отсутствии Multica использует внутренний BoardManager как fallback.

## Когда использовать
- Нужно создать задачу в Multica и назначить агента
- Требуется обновить статус задачи через Multica UI
- Интеграция с Multica канбан-доской

## API Endpoint
**URL:** `http://localhost:3000/api/tasks`
**Метод:** POST

## Команда (прямой HTTP)
**Команда:** `curl -X POST http://localhost:3000/api/tasks -H "Content-Type: application/json" -d '{"title":"{title}","agent":"{agent}"}'`

## Входные параметры
| Параметр | Тип | Описание |
|----------|-----|----------|
| `title` | str | Название задачи |
| `agent` | str | Агент: "Hermes", "OpenClaw", "CAMEL" |
| `priority` | str | "low", "medium", "high", "critical" |

## Выходные данные
- `multica_task_id`: ID созданной задачи

## Пример вызова из WorkflowEngine
```json
{
  "name": "create_multica_task",
  "type": "action",
  "action": "call_multica",
  "title": "Analyze: {goal}",
  "agent": "Hermes",
  "priority": "high"
}
```

## Схема запуска Multica
```bash
# Запуск Multica (требует Docker):
docker-compose up multica

# Или локально (требует Go + Node.js):
cd multica && make dev
```

## Примечания
- Multica слушает на localhost:3000 (Next.js frontend)
- Go API на localhost:8080
- При недоступности Multica задача создаётся в internal board
- Результат одинаков: task_id + статус
