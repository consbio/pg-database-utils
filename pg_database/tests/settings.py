import json
import secrets

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

CONFIG = {}
config_file = Path(BASE_DIR / "test_config.json").resolve()
if config_file and config_file.is_file():
    with open(config_file) as f:
        CONFIG = json.loads(f.read())

DEBUG = True
SECRET_KEY = secrets.token_urlsafe(50)

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": CONFIG.get("database-name"),
        "USER": "django",
    }
}
