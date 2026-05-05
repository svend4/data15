# Skill: camel_decompose
## Назначение
Декомпозиция высокоуровневой цели на конкретные подзадачи с помощью CAMEL-AI.
Каждая подзадача назначается нужному агенту (Hermes или OpenClaw).

## Когда использовать
- Задача слишком большая для одного агента
- Нужно распараллелить работу между агентами
- Требуется последовательное выполнение зависимых шагов

## Команда (CAMEL-AI SDK)
**Команда:** `python3 -c "from camel.agents import ChatAgent; ..."`

## Через internal CAMELLayer (fallback)
- Автоматически используется, если camel SDK не установлен
- Декомпозиция: research → analysis → implementation → review

## Входные параметры
| Параметр | Тип | Описание |
|----------|-----|----------|
| `goal` | str | Высокоуровневая цель для декомпозиции |
| `depth` | int | Количество подзадач (1–5, по умолч. 3) |

## Выходные данные
- `camel_tasks`: список словарей `{step, title, agent, id}`

## Пример вызова из WorkflowEngine
```json
{
  "name": "decompose_goal",
  "type": "action",
  "action": "call_camel",
  "goal": "{goal}",
  "depth": 3
}
```

## Результирующие фазы по умолчанию
1. **Research** → OpenClaw (внешний поиск)
2. **Analysis** → Hermes (внутренний анализ)
3. **Implementation** → Hermes (генерация результата)

## Примечания
- При наличии camel-ai SDK используется реальный агент
- Fallback использует встроенный CAMELLayer оркестратора
- Результаты сохраняются в context["camel_tasks"]
