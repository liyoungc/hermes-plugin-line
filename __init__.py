try:
    from .line_platform.adapter import register
except ImportError:  # pytest/import-from-directory fallback
    from line_platform.adapter import register

__all__ = ["register"]
