"""
agents/smart_camel.py
=====================
Smart CAMEL — LLM-powered декомпозиция целей для Orchestrator v6.

Что изменилось vs CAMELLayer v5:
  ❌ v5: всегда 3 фазы Research→Analysis→Implementation (шаблон)
  ✅ v6: LLM анализирует цель и генерирует уникальный план:
         - число подзадач от 2 до 8 в зависимости от сложности
         - умное назначение агентов (Hermes/OpenClaw) по типу задачи
         - детальные описания для каждого шага
         - карта зависимостей между подзадачами
         - оценка сложности и приоритета

Использование:
    from agents.smart_camel import SmartCAMEL
    from llm.provider import get_provider

    camel = SmartCAMEL(provider=get_provider())
    plan = camel.decompose("сравни Tesla и BYD по финансовым показателям 2025")
    for task in plan.tasks:
        print(task)
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Optional

from llm.provider import LLMProvider, get_provider


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class SubTask:
    step: int
    title: str
    description: str
    agent: str                        # "Hermes" | "OpenClaw" | "CAMEL"
    priority: str = "medium"          # low|medium|high|critical
    layer: str = "execution"          # execution|operational|strategic
    depends_on: list[int] = field(default_factory=list)  # шаги-зависимости
    complexity: int = 5               # 1-10
    estimated_minutes: int = 5

    def to_board_kwargs(self) -> dict:
        """Параметры для SQLiteBoardManager.add_task()"""
        return {
            "description": self.description,
            "agent": self.agent,
            "priority": self.priority,
            "layer": self.layer,
            "complexity": self.complexity,
            "tags": [f"step-{self.step}", "camel-generated"],
        }


@dataclass
class DecompositionPlan:
    goal: str
    tasks: list[SubTask]
    strategy: str           # краткое объяснение выбранной стратегии
    estimated_total_minutes: int
    provider_used: str
    latency_ms: float

    def summary(self) -> str:
        lines = [
            f"🎯 Цель: {self.goal}",
            f"📋 Стратегия: {self.strategy}",
            f"⏱  Оценка: ~{self.estimated_total_minutes} мин | {len(self.tasks)} подзадач",
            f"🤖 LLM: {self.provider_used} ({self.latency_ms:.0f}ms)",
            "",
        ]
        for t in self.tasks:
            deps = f" [зависит от: {t.depends_on}]" if t.depends_on else ""
            lines.append(
                f"  {t.step}. [{t.agent:9s}] {t.title} (сложн={t.complexity}){deps}"
            )
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# SmartCAMEL
# ---------------------------------------------------------------------------

DECOMPOSE_SYSTEM = """Ты — стратегический планировщик мультиагентной системы.
Тебе дают высокоуровневую цель. Твоя задача — разбить её на конкретные подзадачи
для двух исполнительных агентов:
  • OpenClaw — умеет: веб-поиск, новости, биржевые данные, внешние API
  • Hermes   — умеет: анализ данных, написание отчётов, синтез, код

Правила:
  1. От 2 до 8 подзадач (не больше — параллелизм важнее детализации)
  2. Каждая подзадача конкретна и выполнима одним агентом
  3. Укажи зависимости: если задача 3 нужна данные из задачи 1, depends_on=[1]
  4. Назначай OpenClaw для сбора данных, Hermes — для анализа и синтеза
  5. Первые задачи (независимые) выполняются параллельно — это ускоряет работу
  6. Оцени complexity (1-10) реально, estimated_minutes честно

Отвечай СТРОГО в JSON, без markdown, без пояснений вне JSON:
{
  "strategy": "краткое описание подхода (1-2 предложения)",
  "estimated_total_minutes": <int>,
  "tasks": [
    {
      "step": 1,
      "title": "Краткое название",
      "description": "Что именно нужно сделать — конкретно",
      "agent": "OpenClaw",
      "priority": "high",
      "layer": "execution",
      "depends_on": [],
      "complexity": 4,
      "estimated_minutes": 3
    }
  ]
}"""


class SmartCAMEL:
    """
    LLM-powered декомпозиция целей.

    Автоматически определяет:
    - сколько подзадач нужно (2-8)
    - какой агент подходит для каждой
    - порядок и зависимости
    - приоритеты и сложность
    """

    def __init__(
        self,
        provider: Optional[LLMProvider] = None,
        temperature: float = 0.3,   # низкая температура для структурированного вывода
    ) -> None:
        self._provider = provider or get_provider()
        self._temperature = temperature
        self._cache: dict[str, DecompositionPlan] = {}

    def decompose(
        self,
        goal: str,
        context: str = "",
        depth_hint: int = 0,        # 0 = авто, 1-8 = принудительное число задач
        use_cache: bool = True,
    ) -> DecompositionPlan:
        """
        Разбить цель на подзадачи.

        Args:
            goal:       Высокоуровневая цель пользователя
            context:    Дополнительный контекст (отрасль, ограничения и т.д.)
            depth_hint: Желаемое число подзадач (0 = решает LLM)
            use_cache:  Кэшировать результат для одинаковых целей

        Returns:
            DecompositionPlan с полным планом выполнения
        """
        cache_key = f"{goal}|{depth_hint}"
        if use_cache and cache_key in self._cache:
            plan = self._cache[cache_key]
            plan.latency_ms = 0
            return plan

        prompt = self._build_prompt(goal, context, depth_hint)

        t0 = time.perf_counter()
        try:
            response = self._provider.complete(
                [
                    {"role": "system", "content": DECOMPOSE_SYSTEM},
                    {"role": "user", "content": prompt},
                ],
                temperature=self._temperature,
                max_tokens=2048,
            )
            latency = (time.perf_counter() - t0) * 1000
            plan = self._parse_response(goal, response.content, latency, response.provider)
        except Exception as e:
            latency = (time.perf_counter() - t0) * 1000
            print(f"[SmartCAMEL] LLM failed ({e}), using structural fallback")
            plan = self._structural_fallback(goal, latency)

        if use_cache:
            self._cache[cache_key] = plan

        return plan

    def decompose_and_create(
        self,
        goal: str,
        board,          # SQLiteBoardManager instance
        context: str = "",
    ) -> list:
        """Декомпозировать и сразу создать задачи на доске."""
        plan = self.decompose(goal, context)
        created = []
        id_map: dict[int, str] = {}  # step → task_id для зависимостей

        for subtask in plan.tasks:
            # Разрешить зависимости в реальные task_id
            real_deps = [id_map[s] for s in subtask.depends_on if s in id_map]
            task = board.add_task(
                title=subtask.title,
                dependencies=real_deps,
                **subtask.to_board_kwargs(),
            )
            id_map[subtask.step] = task.id
            created.append(task)

        return created

    # ------------------------------------------------------------------
    # Парсинг ответа LLM
    # ------------------------------------------------------------------

    def _build_prompt(self, goal: str, context: str, depth_hint: int) -> str:
        lines = [f"Цель: {goal}"]
        if context:
            lines.append(f"Контекст: {context}")
        if depth_hint > 0:
            lines.append(f"Пожалуйста, сгенерируй ровно {depth_hint} подзадачи.")
        lines.append("\nРазбей эту цель на подзадачи.")
        return "\n".join(lines)

    def _parse_response(
        self, goal: str, raw: str, latency: float, provider_name: str
    ) -> DecompositionPlan:
        # Извлечь JSON из ответа (LLM иногда оборачивает в ```json ... ```)
        json_str = self._extract_json(raw)

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise ValueError(f"[SmartCAMEL] JSON parse error: {e}\nRaw: {raw[:500]}")

        tasks_raw = data.get("tasks", [])
        if not tasks_raw:
            raise ValueError("[SmartCAMEL] Empty tasks list in response")

        subtasks = []
        for t in tasks_raw:
            agent = self._normalize_agent(t.get("agent", "Hermes"))
            subtasks.append(SubTask(
                step=int(t.get("step", len(subtasks) + 1)),
                title=str(t.get("title", "Untitled")),
                description=str(t.get("description", "")),
                agent=agent,
                priority=self._normalize_priority(t.get("priority", "medium")),
                layer=str(t.get("layer", "execution")),
                depends_on=[int(d) for d in t.get("depends_on", [])],
                complexity=max(1, min(10, int(t.get("complexity", 5)))),
                estimated_minutes=max(1, int(t.get("estimated_minutes", 5))),
            ))

        return DecompositionPlan(
            goal=goal,
            tasks=subtasks,
            strategy=str(data.get("strategy", "LLM-generated plan")),
            estimated_total_minutes=int(data.get("estimated_total_minutes", 15)),
            provider_used=provider_name,
            latency_ms=latency,
        )

    def _extract_json(self, text: str) -> str:
        """Извлечь JSON из текста (убрать markdown code blocks)."""
        # Попробовать ```json ... ```
        m = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
        if m:
            return m.group(1)
        # Найти первый { ... }
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            return m.group(0)
        return text.strip()

    def _normalize_agent(self, agent: str) -> str:
        agent = agent.strip().lower()
        if "open" in agent or "claw" in agent or "search" in agent or "web" in agent:
            return "OpenClaw"
        if "hermes" in agent or "analys" in agent or "internal" in agent:
            return "Hermes"
        if "camel" in agent:
            return "CAMEL"
        return "Hermes"  # default

    def _normalize_priority(self, p: str) -> str:
        p = str(p).lower()
        if p in ("critical", "high", "medium", "low"):
            return p
        if "crit" in p:
            return "critical"
        if "high" in p or "высок" in p:
            return "high"
        if "low" in p or "низк" in p:
            return "low"
        return "medium"

    # ------------------------------------------------------------------
    # Структурный fallback (нет LLM)
    # ------------------------------------------------------------------

    def _structural_fallback(self, goal: str, latency: float) -> DecompositionPlan:
        """
        Умный fallback: анализирует ключевые слова в цели
        для более релевантной декомпозиции чем v5.
        """
        goal_lower = goal.lower()

        # Определить тип задачи по ключевым словам
        is_research = any(w in goal_lower for w in [
            "найди", "поиск", "исследуй", "find", "search", "news", "новости",
            "данные", "statistics", "рынок", "market"
        ])
        is_analysis = any(w in goal_lower for w in [
            "анализ", "сравни", "оцени", "analyse", "compare", "evaluate",
            "swot", "report", "отчёт"
        ])
        is_code = any(w in goal_lower for w in [
            "код", "скрипт", "функция", "code", "script", "function",
            "implement", "напиши", "write"
        ])
        is_monitoring = any(w in goal_lower for w in [
            "мониторинг", "отслеживай", "monitor", "watch", "alert", "уведомление"
        ])

        tasks = []

        if is_research or (not is_analysis and not is_code):
            tasks.append(SubTask(
                step=1,
                title=f"Поиск данных: {goal[:50]}",
                description=f"Найти актуальную информацию по теме: {goal}",
                agent="OpenClaw",
                priority="high",
                complexity=3,
                estimated_minutes=5,
            ))

        if is_analysis or is_research:
            tasks.append(SubTask(
                step=2,
                title="Анализ и структурирование",
                description="Проанализировать полученные данные, выделить ключевые инсайты",
                agent="Hermes",
                priority="high",
                depends_on=[1] if tasks else [],
                complexity=6,
                estimated_minutes=8,
            ))

        if is_code:
            tasks.append(SubTask(
                step=len(tasks) + 1,
                title="Реализация",
                description=f"Написать код/скрипт для: {goal}",
                agent="Hermes",
                priority="high",
                depends_on=list(range(1, len(tasks) + 1)),
                complexity=7,
                estimated_minutes=10,
            ))

        if is_monitoring:
            tasks.append(SubTask(
                step=len(tasks) + 1,
                title="Настройка мониторинга",
                description="Настроить алерты и периодические проверки",
                agent="Hermes",
                priority="medium",
                depends_on=list(range(1, len(tasks) + 1)),
                complexity=5,
                estimated_minutes=5,
            ))

        # Всегда финальный синтез
        tasks.append(SubTask(
            step=len(tasks) + 1,
            title="Финальный отчёт",
            description="Синтезировать все результаты в структурированный итоговый отчёт",
            agent="Hermes",
            priority="medium",
            depends_on=list(range(1, len(tasks) + 1)),
            complexity=4,
            estimated_minutes=5,
        ))

        return DecompositionPlan(
            goal=goal,
            tasks=tasks,
            strategy="Structural fallback (LLM unavailable) — keyword-based decomposition",
            estimated_total_minutes=sum(t.estimated_minutes for t in tasks),
            provider_used="builtin",
            latency_ms=latency,
        )

    def clear_cache(self) -> None:
        self._cache.clear()

    def __repr__(self) -> str:
        return f"<SmartCAMEL provider={self._provider!r} cached={len(self._cache)}>"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from llm.provider import get_provider

    goal = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Сравни Tesla и BYD по финансовым показателям за 2025 год"

    camel = SmartCAMEL(provider=get_provider())
    print(f"Decomposing: {goal!r}\n")
    plan = camel.decompose(goal)
    print(plan.summary())
