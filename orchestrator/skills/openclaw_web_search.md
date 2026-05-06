# Skill: openclaw_web_search
## Назначение
Поиск информации во внешних источниках через OpenClaw Agent.
Используется для получения актуальных данных из интернета, API, новостей.

## Когда использовать
- Нужны актуальные новости или данные из интернета
- Требуется обращение к внешним API (биржи, погода, и т.д.)
- Нужен поиск по конкретному запросу в веб

## Команда
**Команда:** `openclaw agent --local --message "Search: {goal}" --session-id orch-{timestamp}`

## Через runner script
**Команда:** `bash openclaw_runner.sh --prompt "Search: {goal}" --timeout 120`

## Входные параметры
| Параметр | Тип | Описание |
|----------|-----|----------|
| `goal` | str | Поисковый запрос или задача |
| `timeout` | int | Таймаут в секундах (по умолч. 120) |

## Выходные данные
- `openclaw_result`: строка с результатами поиска

## Пример вызова из WorkflowEngine
```json
{
  "name": "web_research",
  "type": "action",
  "action": "call_openclaw",
  "prompt": "Find latest news about: {goal}",
  "critical": true
}
```

## Примечания
- OpenClaw работает локально, не требует внешних API-ключей
- Поддерживает 20+ каналов и протоколов (MCP)
- При ошибке проверить PATH и nvm окружение через openclaw_runner.sh
