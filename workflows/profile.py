import json
import os

from spejder.config import AppConfig
from spejder.core import DEFAULT_PROFILE_PATH


def init_profile(path: str = None, force: bool = False):
    if os.path.exists(path) and not force:
        print("Profile already exists. Use --force to overwrite:", path)
        return
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    config = AppConfig.load(DEFAULT_PROFILE_PATH)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config.model_dump(), f, ensure_ascii=False, indent=2)
    print("Created profile:", path)



