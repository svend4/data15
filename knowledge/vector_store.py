"""
knowledge/vector_store.py
=========================
Векторная база знаний для Orchestrator v6.

Что изменилось vs KnowledgeBase v5:
  ❌ v5: полнотекстовый поиск по JSON (ключевые слова, нет семантики)
  ✅ v6: семантический поиск — находит похожие результаты даже без точных слов
         "анализ рынка EV" → находит статьи про "электромобили" и "конкурентов Tesla"

Бекенды (в порядке приоритета):
  1. ChromaDB  — production-ready векторная БД (pip install chromadb)
  2. SQLite+TF-IDF — встроенный fallback (только stdlib + math)

Применение:
  • Помнить предыдущие результаты анализа и передавать как контекст агентам
  • "Я уже исследовал это 3 дня назад, вот результат" → нет повторных запросов
  • Поиск релевантных знаний для обогащения запросов к LLM
"""

from __future__ import annotations

import json
import math
import os
import re
import sqlite3
import time
import uuid
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class Article:
    id: str
    title: str
    content: str
    category: str = "general"
    tags: list[str] = field(default_factory=list)
    source: str = ""              # "task:T-001", "workflow:research_and_analyze", etc.
    created_at: str = ""
    view_count: int = 0
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "title": self.title,
            "content": self.content,
            "category": self.category,
            "tags": self.tags,
            "source": self.source,
            "created_at": self.created_at,
            "view_count": self.view_count,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Article":
        return cls(
            id=d.get("id", str(uuid.uuid4())),
            title=d.get("title", ""),
            content=d.get("content", ""),
            category=d.get("category", "general"),
            tags=d.get("tags", []),
            source=d.get("source", ""),
            created_at=d.get("created_at", ""),
            view_count=d.get("view_count", 0),
            metadata=d.get("metadata", {}),
        )


@dataclass
class SearchResult:
    article: Article
    score: float            # 0.0 – 1.0 (выше = релевантнее)
    backend: str


# ---------------------------------------------------------------------------
# Abstract backend
# ---------------------------------------------------------------------------

class _VectorBackend(ABC):
    @abstractmethod
    def add(self, article: Article) -> None: ...

    @abstractmethod
    def search(self, query: str, top_k: int, category: Optional[str]) -> list[SearchResult]: ...

    @abstractmethod
    def delete(self, article_id: str) -> bool: ...

    @abstractmethod
    def count(self) -> int: ...

    @abstractmethod
    def list_all(self, category: Optional[str]) -> list[Article]: ...


# ---------------------------------------------------------------------------
# Backend 1: ChromaDB
# ---------------------------------------------------------------------------

class _ChromaBackend(_VectorBackend):
    """ChromaDB с sentence-transformers для эмбеддингов."""

    def __init__(self, persist_dir: str) -> None:
        import chromadb
        from chromadb.utils import embedding_functions

        self._client = chromadb.PersistentClient(path=persist_dir)
        # Используем встроенные sentence-transformers (или all-MiniLM-L6-v2)
        try:
            self._ef = embedding_functions.SentenceTransformerEmbeddingFunction(
                model_name="all-MiniLM-L6-v2"
            )
        except Exception:
            self._ef = embedding_functions.DefaultEmbeddingFunction()

        self._col = self._client.get_or_create_collection(
            name="orchestrator_knowledge",
            embedding_function=self._ef,
            metadata={"hnsw:space": "cosine"},
        )

    def _doc_text(self, article: Article) -> str:
        return f"{article.title}\n{article.content}"

    def add(self, article: Article) -> None:
        self._col.upsert(
            ids=[article.id],
            documents=[self._doc_text(article)],
            metadatas=[{
                "title": article.title,
                "category": article.category,
                "source": article.source,
                "tags": json.dumps(article.tags),
                "created_at": article.created_at,
                "view_count": str(article.view_count),
            }],
        )

    def search(self, query: str, top_k: int = 5, category: Optional[str] = None) -> list[SearchResult]:
        where = {"category": category} if category else None
        results = self._col.query(
            query_texts=[query],
            n_results=min(top_k, max(1, self.count())),
            where=where,
        )

        articles = []
        ids = results["ids"][0]
        distances = results["distances"][0]
        metadatas = results["metadatas"][0]
        documents = results["documents"][0]

        for i, (aid, dist, meta, doc) in enumerate(zip(ids, distances, metadatas, documents)):
            score = max(0.0, 1.0 - dist)  # cosine distance → similarity
            title = meta.get("title", "")
            content = doc.replace(title, "", 1).strip()
            article = Article(
                id=aid,
                title=title,
                content=content,
                category=meta.get("category", "general"),
                tags=json.loads(meta.get("tags", "[]")),
                source=meta.get("source", ""),
                created_at=meta.get("created_at", ""),
                view_count=int(meta.get("view_count", 0)),
            )
            articles.append(SearchResult(article=article, score=score, backend="chromadb"))

        return sorted(articles, key=lambda x: x.score, reverse=True)

    def delete(self, article_id: str) -> bool:
        try:
            self._col.delete(ids=[article_id])
            return True
        except Exception:
            return False

    def count(self) -> int:
        return self._col.count()

    def list_all(self, category: Optional[str] = None) -> list[Article]:
        where = {"category": category} if category else None
        try:
            result = self._col.get(where=where, include=["metadatas", "documents"])
        except Exception:
            return []
        articles = []
        for aid, meta, doc in zip(result["ids"], result["metadatas"], result["documents"]):
            title = meta.get("title", "")
            content = doc.replace(title, "", 1).strip()
            articles.append(Article(
                id=aid,
                title=title,
                content=content,
                category=meta.get("category", "general"),
                tags=json.loads(meta.get("tags", "[]")),
                source=meta.get("source", ""),
                created_at=meta.get("created_at", ""),
                view_count=int(meta.get("view_count", 0)),
            ))
        return articles


# ---------------------------------------------------------------------------
# Backend 2: SQLite + TF-IDF (встроенный fallback)
# ---------------------------------------------------------------------------

class _SQLiteTFIDFBackend(_VectorBackend):
    """
    Fallback без внешних зависимостей.
    TF-IDF реализован вручную на stdlib.
    Качество ниже чем ChromaDB, но работает везде.
    """

    def __init__(self, db_path: str) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()
        self._idf_cache: dict[str, float] = {}
        self._idf_dirty = True

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.executescript("""
                CREATE TABLE IF NOT EXISTS kb_articles (
                    id         TEXT PRIMARY KEY,
                    title      TEXT NOT NULL,
                    content    TEXT NOT NULL,
                    category   TEXT DEFAULT 'general',
                    tags       TEXT DEFAULT '[]',
                    source     TEXT DEFAULT '',
                    created_at TEXT,
                    view_count INTEGER DEFAULT 0,
                    metadata   TEXT DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_kb_category ON kb_articles(category);

                CREATE VIRTUAL TABLE IF NOT EXISTS kb_fts USING fts5(
                    id UNINDEXED, title, content,
                    content='kb_articles', content_rowid='rowid'
                );
                CREATE TRIGGER IF NOT EXISTS kb_ai AFTER INSERT ON kb_articles BEGIN
                    INSERT INTO kb_fts(rowid,id,title,content)
                    VALUES(new.rowid,new.id,new.title,new.content);
                END;
                CREATE TRIGGER IF NOT EXISTS kb_ad AFTER DELETE ON kb_articles BEGIN
                    DELETE FROM kb_fts WHERE id=old.id;
                END;
                CREATE TRIGGER IF NOT EXISTS kb_au AFTER UPDATE ON kb_articles BEGIN
                    UPDATE kb_fts SET title=new.title,content=new.content WHERE id=new.id;
                END;
            """)

    # TF-IDF helpers
    def _tokenize(self, text: str) -> list[str]:
        return re.findall(r"\b[а-яёa-z]{2,}\b", text.lower())

    def _tf(self, tokens: list[str]) -> dict[str, float]:
        if not tokens:
            return {}
        counter = Counter(tokens)
        total = len(tokens)
        return {t: c / total for t, c in counter.items()}

    def _compute_idf(self) -> None:
        rows = self._conn.execute("SELECT title, content FROM kb_articles").fetchall()
        n = len(rows)
        if n == 0:
            self._idf_cache = {}
            return
        df: Counter = Counter()
        for row in rows:
            tokens = set(self._tokenize(f"{row['title']} {row['content']}"))
            df.update(tokens)
        self._idf_cache = {
            t: math.log((n + 1) / (c + 1)) + 1
            for t, c in df.items()
        }
        self._idf_dirty = False

    def _score(self, query: str, title: str, content: str) -> float:
        if self._idf_dirty:
            self._compute_idf()
        q_tokens = self._tokenize(query)
        doc_tokens = self._tokenize(f"{title} {content}")
        tf = self._tf(doc_tokens)
        score = sum(
            tf.get(t, 0) * self._idf_cache.get(t, 0)
            for t in q_tokens
        )
        # Нормировать к [0, 1]
        max_possible = sum(self._idf_cache.get(t, 0) for t in q_tokens) or 1
        return min(1.0, score / max_possible)

    def add(self, article: Article) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT OR REPLACE INTO kb_articles "
                "(id,title,content,category,tags,source,created_at,view_count,metadata) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    article.id, article.title, article.content,
                    article.category, json.dumps(article.tags),
                    article.source, article.created_at,
                    article.view_count, json.dumps(article.metadata),
                ),
            )
        self._idf_dirty = True

    def search(self, query: str, top_k: int = 5, category: Optional[str] = None) -> list[SearchResult]:
        # Сначала FTS для быстрой предварительной фильтрации
        where = "WHERE a.category = ?" if category else ""
        params: list[Any] = [category] if category else []

        try:
            fts_query = " OR ".join(self._tokenize(query)[:5]) or query
            rows = self._conn.execute(
                f"""
                SELECT a.* FROM kb_articles a
                JOIN kb_fts f ON a.id = f.id
                WHERE kb_fts MATCH ? {('AND a.category=?' if category else '')}
                ORDER BY rank LIMIT ?
                """,
                ([fts_query] + ([category] if category else []) + [top_k * 3]),
            ).fetchall()
        except Exception:
            rows = self._conn.execute(
                f"SELECT * FROM kb_articles {where} LIMIT ?",
                params + [top_k * 3],
            ).fetchall()

        results = []
        for row in rows:
            score = self._score(query, row["title"], row["content"])
            article = Article(
                id=row["id"],
                title=row["title"],
                content=row["content"],
                category=row["category"],
                tags=json.loads(row["tags"] or "[]"),
                source=row["source"] or "",
                created_at=row["created_at"] or "",
                view_count=row["view_count"] or 0,
                metadata=json.loads(row["metadata"] or "{}"),
            )
            results.append(SearchResult(article=article, score=score, backend="sqlite-tfidf"))

        results.sort(key=lambda x: x.score, reverse=True)
        return results[:top_k]

    def delete(self, article_id: str) -> bool:
        with self._conn:
            cur = self._conn.execute("DELETE FROM kb_articles WHERE id=?", (article_id,))
        self._idf_dirty = True
        return cur.rowcount > 0

    def count(self) -> int:
        return self._conn.execute("SELECT COUNT(*) FROM kb_articles").fetchone()[0]

    def list_all(self, category: Optional[str] = None) -> list[Article]:
        if category:
            rows = self._conn.execute(
                "SELECT * FROM kb_articles WHERE category=? ORDER BY created_at DESC", (category,)
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM kb_articles ORDER BY created_at DESC"
            ).fetchall()
        return [
            Article(
                id=r["id"], title=r["title"], content=r["content"],
                category=r["category"], tags=json.loads(r["tags"] or "[]"),
                source=r["source"] or "", created_at=r["created_at"] or "",
                view_count=r["view_count"] or 0,
                metadata=json.loads(r["metadata"] or "{}"),
            )
            for r in rows
        ]


# ---------------------------------------------------------------------------
# VectorKnowledgeBase — публичный API
# ---------------------------------------------------------------------------

class VectorKnowledgeBase:
    """
    Семантическая база знаний.

    Автоматически выбирает ChromaDB или SQLite+TF-IDF.
    Публичный API совместим с KnowledgeBase из v5, плюс новые методы.
    """

    def __init__(
        self,
        persist_dir: str = "orchestrator/db/knowledge",
        backend: str = "auto",    # "auto" | "chromadb" | "sqlite"
    ) -> None:
        Path(persist_dir).mkdir(parents=True, exist_ok=True)
        self._persist_dir = persist_dir
        self._backend = self._init_backend(backend, persist_dir)
        print(f"[KnowledgeBase] Backend: {self._backend.__class__.__name__}")

    def _init_backend(self, backend: str, persist_dir: str) -> _VectorBackend:
        if backend == "chromadb" or backend == "auto":
            try:
                import chromadb  # noqa: F401
                return _ChromaBackend(persist_dir)
            except ImportError:
                if backend == "chromadb":
                    raise RuntimeError("chromadb not installed: pip install chromadb sentence-transformers")
                print("[KnowledgeBase] ChromaDB not available, using SQLite+TF-IDF fallback")
        return _SQLiteTFIDFBackend(os.path.join(persist_dir, "knowledge.db"))

    # ------------------------------------------------------------------
    # Совместимость с KnowledgeBase v5
    # ------------------------------------------------------------------

    def add_article(
        self,
        title: str,
        content: str,
        category: str = "general",
        tags: Optional[list[str]] = None,
        source: str = "",
        metadata: Optional[dict] = None,
    ) -> Article:
        article = Article(
            id=str(uuid.uuid4()),
            title=title,
            content=content,
            category=category,
            tags=tags or [],
            source=source,
            created_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            metadata=metadata or {},
        )
        self._backend.add(article)
        return article

    def search(self, query: str, category: Optional[str] = None, top_k: int = 5) -> list[SearchResult]:
        """Семантический поиск по базе знаний."""
        return self._backend.search(query, top_k=top_k, category=category)

    def get_article(self, article_id: str) -> Optional[Article]:
        results = self._backend.list_all()
        for a in results:
            if a.id == article_id:
                return a
        return None

    def get_by_category(self, category: str) -> list[Article]:
        return self._backend.list_all(category=category)

    def get_popular(self, limit: int = 5) -> list[Article]:
        all_articles = self._backend.list_all()
        return sorted(all_articles, key=lambda a: a.view_count, reverse=True)[:limit]

    def delete(self, article_id: str) -> bool:
        return self._backend.delete(article_id)

    def count(self) -> int:
        return self._backend.count()

    # ------------------------------------------------------------------
    # Новые возможности v6
    # ------------------------------------------------------------------

    def store_task_result(
        self,
        task_id: str,
        title: str,
        result: str,
        agent: str = "Hermes",
        tags: Optional[list[str]] = None,
    ) -> Article:
        """Сохранить результат выполнения задачи для последующего поиска."""
        return self.add_article(
            title=f"[{task_id}] {title}",
            content=result,
            category="task_results",
            tags=(tags or []) + [agent.lower(), task_id],
            source=f"task:{task_id}",
            metadata={"task_id": task_id, "agent": agent},
        )

    def find_similar_tasks(self, goal: str, top_k: int = 3) -> list[SearchResult]:
        """Найти похожие ранее выполненные задачи."""
        return self.search(goal, category="task_results", top_k=top_k)

    def get_context_for_goal(self, goal: str, max_chars: int = 2000) -> str:
        """
        Сформировать контекст из базы знаний для передачи в LLM.
        Возвращает релевантные фрагменты предыдущих результатов.
        """
        results = self.find_similar_tasks(goal, top_k=3)
        if not results:
            return ""

        parts = ["=== Релевантные предыдущие результаты ==="]
        total_chars = 0
        for r in results:
            if r.score < 0.1:
                continue
            snippet = r.article.content[:500]
            entry = f"\n[{r.article.title}] (схожесть={r.score:.2f}):\n{snippet}..."
            if total_chars + len(entry) > max_chars:
                break
            parts.append(entry)
            total_chars += len(entry)

        return "\n".join(parts) if len(parts) > 1 else ""

    def migrate_from_json(self, json_path: str = "orchestrator/state/knowledge_base.json") -> int:
        """Импортировать статьи из knowledge_base.json v5."""
        path = Path(json_path)
        if not path.exists():
            return 0
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        articles = data.get("articles", [])
        migrated = 0
        for a in articles:
            try:
                self.add_article(
                    title=a.get("title", "Untitled"),
                    content=a.get("content", ""),
                    category=a.get("category", "general"),
                    tags=a.get("tags", []),
                    source=a.get("source", "migration"),
                )
                migrated += 1
            except Exception as e:
                print(f"[KB migrate] Skipped: {e}")
        print(f"[KB migrate] Перенесено {migrated}/{len(articles)} статей")
        return migrated

    def __repr__(self) -> str:
        return (
            f"<VectorKnowledgeBase backend={self._backend.__class__.__name__} "
            f"articles={self.count()} persist={self._persist_dir!r}>"
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    kb = VectorKnowledgeBase()
    print(kb)

    if "--migrate" in sys.argv:
        kb.migrate_from_json()
    elif len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        results = kb.search(query, top_k=5)
        print(f"\nSearch: {query!r}")
        for r in results:
            print(f"  [{r.score:.3f}] {r.article.title} ({r.article.category})")
    else:
        # Demo
        kb.add_article("Tesla Q1 2025", "Tesla revenue $21.3B, EV deliveries 337k", "finance")
        kb.add_article("BYD Market Share", "BYD captured 18% global EV market in 2025", "finance")
        kb.add_article("AI Trends 2025", "LLM adoption in enterprise grew 340%", "tech")
        results = kb.search("электромобили и конкуренция")
        print("\nDemo search: 'электромобили и конкуренция'")
        for r in results:
            print(f"  [{r.score:.3f}] {r.article.title}")
