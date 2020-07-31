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

DATABASES = {}

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django.contrib.gis',
]
