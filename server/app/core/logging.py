import logging
import sys

def setup_logging(level: int = logging.INFO):
    """Configure root logger with a simple, readable format."""
    fmt = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(fmt))

    root = logging.getLogger()
    root.setLevel(level)

    # Avoid adding duplicate handlers during reloads
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        root.addHandler(handler)

def get_logger(name: str = None):
    return logging.getLogger(name or __name__)
