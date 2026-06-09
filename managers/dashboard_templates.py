"""Jinja2 environment for dashboard HTML templates."""

import os

from jinja2 import Environment, FileSystemLoader

jinja_env = Environment(
    loader=FileSystemLoader(
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "templates")
    )
)
