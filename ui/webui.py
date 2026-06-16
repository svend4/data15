"""
ui/webui.py
===========
Streamlit WebUI для Orchestrator v6.

Запуск:
    streamlit run ui/webui.py

Вкладки:
  📋 Kanban     — визуальная доска задач (pending→in_progress→done/failed)
  🤖 Agenten    — управление агентами, ручной запуск задачи
  📚 Wissen     — поиск по базе знаний
  📊 Metriken   — статистика производительности и health-check
  ⚙️  Konfig     — текущие настройки провайдеров, просмотр конфига

Что изменилось vs v5 (нет UI вообще):
  ❌ v5: только CLI + JSON файлы, нет возможности наблюдать за работой
  ✅ v6: живой дашборд с автообновлением каждые 5 секунд
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import Optional

# Добавить корень проекта в path для импортов
_root = Path(__file__).parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

# ---------------------------------------------------------------------------
# Ленивые импорты (streamlit может отсутствовать)
# ---------------------------------------------------------------------------

def _require_streamlit():
    try:
        import streamlit as st
        return st
    except ImportError:
        print("Streamlit не установлен. Запустите: pip install streamlit")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

STATUS_EMOJI = {
    "queued":    "⏳",
    "running":   "🔄",
    "completed": "✅",
    "failed":    "❌",
    "blocked":   "🔒",
    # backward-compat aliases (old vocabulary)
    "pending":     "⏳",
    "in_progress": "🔄",
    "done":        "✅",
    "cancelled":   "🚫",
}

PRIORITY_COLOR = {
    "critical": "#FF4444",
    "high":     "#FF8C00",
    "medium":   "#1E90FF",
    "low":      "#808080",
}

AGENT_EMOJI = {
    "Hermes":   "🧠",
    "OpenClaw": "🔍",
    "CAMEL":    "🐪",
}

REFRESH_INTERVAL = 5   # секунды


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_board():
    """Загрузить SQLiteBoardManager."""
    try:
        from storage.sqlite_board import SQLiteBoardManager
        return SQLiteBoardManager()
    except Exception as e:
        return None, str(e)


def _load_knowledge():
    """Загрузить VectorKnowledgeBase."""
    try:
        from knowledge.vector_store import VectorKnowledgeBase
        return VectorKnowledgeBase()
    except Exception as e:
        return None, str(e)


def _load_provider():
    """Загрузить LLM провайдер."""
    try:
        from llm.provider import get_provider
        return get_provider()
    except Exception as e:
        return None, str(e)


def _load_config() -> dict:
    paths = ["orchestrator/state/config.json", "state/config.json", "config.json"]
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            continue
    return {}


def _fmt_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds:.0f}s"
    return f"{seconds/60:.1f}m"


def _fmt_time(iso: Optional[str]) -> str:
    if not iso:
        return "—"
    return iso[:16].replace("T", " ")


# ---------------------------------------------------------------------------
# Страница: Kanban Board
# ---------------------------------------------------------------------------

def page_kanban(st, board) -> None:
    st.subheader("📋 Канбан-доска")

    # Кнопки управления
    col_refresh, col_filter, _ = st.columns([1, 2, 4])
    with col_refresh:
        if st.button("🔄 Обновить"):
            st.rerun()
    with col_filter:
        agent_filter = st.selectbox(
            "Агент",
            ["Все", "Hermes", "OpenClaw", "CAMEL"],
            label_visibility="collapsed",
        )

    # Фильтр по агенту
    agent = None if agent_filter == "Все" else agent_filter

    # Загрузить задачи по статусам (sqlite_board vocabulary)
    statuses = ["queued", "running", "completed", "failed"]
    cols = st.columns(4)
    col_labels = ["⏳ Ожидание", "🔄 Выполнение", "✅ Готово", "❌ Ошибка"]

    for col, status, label in zip(cols, statuses, col_labels):
        with col:
            tasks = board.list_tasks(status=status, agent=agent, limit=20)
            st.markdown(f"**{label}** ({len(tasks)})")
            for task in tasks:
                _task_card(st, task, board)


def _task_card(st, task, board) -> None:
    """Карточка задачи в Kanban-колонке."""
    agent_em = AGENT_EMOJI.get(task.agent, "🤖")
    prio_color = PRIORITY_COLOR.get(task.priority, "#808080")

    with st.container():
        st.markdown(f"""
<div style="
    border-left: 4px solid {prio_color};
    background: rgba(255,255,255,0.05);
    border-radius: 6px;
    padding: 8px 10px;
    margin-bottom: 8px;
    font-size: 0.85em;
">
<b>{agent_em} {task.title[:40]}</b><br>
<span style="color:#888">{task.id[:8]} · {task.priority}</span>
</div>
""", unsafe_allow_html=True)

        # Кнопки действий (только для незавершённых)
        if task.status not in ("completed", "failed", "blocked"):
            c1, c2 = st.columns(2)
            with c1:
                if st.button("▶ Старт", key=f"start_{task.id}", use_container_width=True):
                    board.update_status(task.id, "running")
                    st.rerun()
            with c2:
                if st.button("✖ Отмена", key=f"cancel_{task.id}", use_container_width=True):
                    board.update_status(task.id, "failed")  # closest terminal state
                    st.rerun()


# ---------------------------------------------------------------------------
# Страница: Agents
# ---------------------------------------------------------------------------

def page_agents(st, board, provider) -> None:
    st.subheader("🤖 Управление агентами")

    # Ручная постановка задачи
    with st.expander("➕ Новая задача", expanded=True):
        title = st.text_input("Название")
        desc  = st.text_area("Описание", height=80)
        c1, c2, c3 = st.columns(3)
        agent    = c1.selectbox("Агент",     ["Hermes", "OpenClaw", "CAMEL"])
        priority = c2.selectbox("Приоритет", ["medium", "high", "critical", "low"])
        layer    = c3.selectbox("Слой",      ["execution", "operational", "strategic"])

        if st.button("Создать задачу", type="primary", disabled=not title):
            task = board.add_task(
                title=title,
                description=desc,
                agent=agent,
                priority=priority,
                layer=layer,
            )
            st.success(f"✅ Создана: {task.id}")
            time.sleep(1)
            st.rerun()

    # Smart CAMEL декомпозиция
    with st.expander("🐪 Smart CAMEL — декомпозиция цели"):
        goal = st.text_input("Цель для декомпозиции")
        if st.button("Декомпозировать", disabled=not goal):
            with st.spinner("CAMEL анализирует цель..."):
                try:
                    from agents.smart_camel import SmartCAMEL
                    camel = SmartCAMEL(provider=provider)
                    plan  = camel.decompose(goal)
                    st.success(f"✅ {len(plan.tasks)} подзадач · {plan.provider_used} · {plan.latency_ms:.0f}ms")
                    st.code(plan.summary(), language=None)

                    if st.button("📌 Создать задачи на доске"):
                        camel.decompose_and_create(goal, board)
                        st.success("Задачи созданы!")
                        time.sleep(1)
                        st.rerun()
                except Exception as e:
                    st.error(f"CAMEL ошибка: {e}")

    # Статистика по агентам
    st.markdown("---")
    st.markdown("**Статистика агентов**")
    stats = board.get_stats()
    agents_data = stats.get("by_agent", {})
    if agents_data:
        for ag, counts in agents_data.items():
            em = AGENT_EMOJI.get(ag, "🤖")
            total = sum(counts.values())
            done  = counts.get("done", 0)
            rate  = f"{done/total*100:.0f}%" if total else "—"
            st.metric(label=f"{em} {ag}", value=f"{total} задач", delta=f"{rate} выполнено")
    else:
        st.info("Задач ещё нет")


# ---------------------------------------------------------------------------
# Страница: Knowledge Base
# ---------------------------------------------------------------------------

def page_knowledge(st, kb) -> None:
    st.subheader("📚 База знаний")

    # Поиск
    query = st.text_input("🔍 Семантический поиск", placeholder="электромобили конкуренция Tesla")
    if query:
        with st.spinner("Ищу..."):
            results = kb.search(query, top_k=8)
        if results:
            for r in results:
                score_pct = f"{r.score*100:.0f}%"
                with st.expander(f"[{score_pct}] {r.article.title} · {r.article.category}"):
                    st.write(r.article.content[:1000])
                    st.caption(f"Источник: {r.article.source} · Бекенд: {r.backend}")
        else:
            st.info("Ничего не найдено")

    # Добавить статью
    with st.expander("➕ Добавить статью"):
        title   = st.text_input("Заголовок", key="kb_title")
        content = st.text_area("Содержание", key="kb_content", height=100)
        cat     = st.text_input("Категория", value="general", key="kb_cat")
        if st.button("Сохранить", disabled=not (title and content)):
            kb.add_article(title, content, category=cat)
            st.success("✅ Добавлено")
            st.rerun()

    # Список категорий
    st.markdown("---")
    total = kb.count()
    st.metric("Всего статей", total)


# ---------------------------------------------------------------------------
# Страница: Metrics
# ---------------------------------------------------------------------------

def page_metrics(st, board) -> None:
    st.subheader("📊 Метрики")

    stats = board.get_stats()

    # Общие числа
    c1, c2, c3, c4 = st.columns(4)
    by_status = stats.get("by_status", {})
    c1.metric("Всего задач",  stats.get("total", 0))
    c2.metric("✅ Выполнено", by_status.get("completed", 0))
    c3.metric("🔄 В работе",  by_status.get("running", 0))
    c4.metric("❌ Ошибок",    by_status.get("failed", 0))

    # По статусам
    st.markdown("**По статусам**")
    if by_status:
        try:
            import streamlit as _st
            _st.bar_chart(by_status)
        except Exception:
            for status, count in by_status.items():
                bar = "█" * min(count, 40)
                st.text(f"{STATUS_EMOJI.get(status,'·')} {status:12s} {bar} {count}")

    # Последние задачи
    st.markdown("---")
    st.markdown("**Последние 10 задач**")
    recent = board.list_tasks(limit=10)
    if recent:
        for task in recent:
            em = STATUS_EMOJI.get(task.status, "·")
            st.text(
                f"{em} [{task.id[:8]}] {task.title[:35]:35s} "
                f"| {task.agent:9s} | {task.priority:8s} | {_fmt_time(task.created_at)}"
            )
    else:
        st.info("Задач ещё нет")


# ---------------------------------------------------------------------------
# Страница: Config
# ---------------------------------------------------------------------------

def page_config(st, provider) -> None:
    st.subheader("⚙️ Конфигурация")

    config = _load_config()

    # LLM провайдер
    st.markdown("**LLM Провайдер**")
    if provider:
        st.info(f"Активный: `{provider!r}`")
        st.metric("Доступен", "✅ Да" if provider.is_available() else "❌ Нет")
    else:
        st.warning("Провайдер не настроен — используется builtin")

    # Конфиг
    if config:
        with st.expander("📄 config.json"):
            # Скрыть секретные ключи
            safe = json.loads(json.dumps(config))
            if "auth" in safe and "users" in safe["auth"]:
                for u in safe["auth"]["users"].values():
                    if "password_hash" in u:
                        u["password_hash"] = "***"
            st.json(safe)
    else:
        st.warning("config.json не найден")

    # Подсказки
    with st.expander("🔑 Как настроить API ключ"):
        st.code("""
# Для Anthropic Claude:
export ANTHROPIC_API_KEY=sk-ant-...

# Для OpenAI:
export OPENAI_API_KEY=sk-...

# Для локального Ollama:
# Запустить ollama run llama3

# config.json:
{
  "llm": {
    "provider": "anthropic",
    "model": "claude-haiku-4-5-20251001",
    "api_key_env": "ANTHROPIC_API_KEY",
    "fallback_chain": ["anthropic", "ollama", "builtin"]
  }
}
        """.strip(), language="bash")


# ---------------------------------------------------------------------------
# Главная функция
# ---------------------------------------------------------------------------

def run_webui() -> None:
    st = _require_streamlit()

    st.set_page_config(
        page_title="Orchestrator v6",
        page_icon="🤖",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Инициализировать компоненты один раз и кэшировать
    @st.cache_resource
    def get_board():
        return _load_board()

    @st.cache_resource
    def get_kb():
        return _load_knowledge()

    @st.cache_resource
    def get_provider():
        return _load_provider()

    board_result = get_board()
    if isinstance(board_result, tuple):
        board, board_err = board_result
    else:
        board, board_err = board_result, None

    kb_result = get_kb()
    if isinstance(kb_result, tuple):
        kb, kb_err = kb_result
    else:
        kb, kb_err = kb_result, None

    provider_result = get_provider()
    if isinstance(provider_result, tuple):
        provider, prov_err = provider_result
    else:
        provider, prov_err = provider_result, None

    # Sidebar
    with st.sidebar:
        st.title("🤖 Orchestrator v6")
        st.caption("Мультиагентная система")
        st.divider()

        page = st.radio(
            "Навигация",
            ["📋 Канбан", "🤖 Агенты", "📚 Знания", "📊 Метрики", "⚙️ Конфиг"],
            label_visibility="collapsed",
        )

        # Статус компонентов
        st.divider()
        st.caption("**Статус**")
        st.text("🗄 SQLite: " + ("✅" if board else f"❌ {board_err}"))
        st.text("📚 KBase:  " + ("✅" if kb    else f"❌ {kb_err}"))
        st.text("🤖 LLM:   " + ("✅" if provider and provider.is_available() else "⚠️ builtin"))

        # Автообновление
        st.divider()
        auto_refresh = st.checkbox("Автообновление (5s)", value=False)
        if auto_refresh:
            time.sleep(REFRESH_INTERVAL)
            st.rerun()

    # Показать страницу
    if board is None:
        st.error(f"❌ SQLite не инициализирован: {board_err}")
        board = None  # Продолжим с None, страницы сами обработают

    if "Канбан" in page:
        if board:
            page_kanban(st, board)
        else:
            st.error("Канбан-доска недоступна")
    elif "Агенты" in page:
        page_agents(st, board, provider)
    elif "Знания" in page:
        if kb:
            page_knowledge(st, kb)
        else:
            st.error(f"База знаний недоступна: {kb_err}")
    elif "Метрики" in page:
        if board:
            page_metrics(st, board)
        else:
            st.error("Метрики недоступны")
    elif "Конфиг" in page:
        page_config(st, provider)


# ---------------------------------------------------------------------------
# Запуск напрямую: python ui/webui.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # При запуске через `python ui/webui.py` вместо `streamlit run ui/webui.py`
    import subprocess
    subprocess.run(
        [sys.executable, "-m", "streamlit", "run", __file__,
         "--server.port", "8501", "--server.headless", "true"],
        check=True,
    )
