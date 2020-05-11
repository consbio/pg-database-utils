import json
import secrets

from pathlib import Path

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = Path(__file__).resolve().parent.parent

CONFIG = {}
config_file = Path(BASE_DIR / "test_config.json").resolve()
if config_file and config_file.is_file():
    with open(config_file) as f:
        CONFIG = json.loads(f.read())


DEBUG = CONFIG.get("debug", True)
SECRET_KEY = CONFIG.get("secret-key", secrets.token_urlsafe(50))


# Database
# https://docs.djangoproject.com/en/2.2/ref/settings/#databases

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": CONFIG.get("database-name", "pg_database"),
        "USER": CONFIG.get("database-user", "postgres"),
    }
}
