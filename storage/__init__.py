"""Storage layer — SQLite backend replacing JSON files."""
from .sqlite_board import SQLiteBoardManager

__all__ = ["SQLiteBoardManager"]
