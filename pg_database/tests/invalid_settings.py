import secrets

DEBUG = True
SECRET_KEY = secrets.token_urlsafe(50)

DATABASES = {
    "default": {
        # Missing database and user name
        "ENGINE": "django.db.backends.postgresql",
        "USER": "",
    }
}
