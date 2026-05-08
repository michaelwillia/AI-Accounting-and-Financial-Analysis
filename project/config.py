"""Application configuration module."""

import os


def get_dashscope_api_key() -> str:
    """Read DashScope API key from environment variable DASHSCOPE_API_KEY."""
    return os.getenv("DASHSCOPE_API_KEY", "").strip()
