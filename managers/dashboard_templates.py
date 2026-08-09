"""Jinja2 environment for dashboard HTML templates."""

import os

from jinja2 import Environment, FileSystemLoader

_TEMPLATES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
_CARD_CORNERS_CSS_PATH = os.path.join(
    _TEMPLATES_DIR, "partials", "dashboard_card_corners.css"
)


def load_dashboard_card_corners_css() -> str:
    with open(_CARD_CORNERS_CSS_PATH, encoding="utf-8") as f:
        return f.read()


jinja_env = Environment(loader=FileSystemLoader(_TEMPLATES_DIR))
