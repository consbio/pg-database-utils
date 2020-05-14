import json
import logging
import os
import pathlib

logger = logging.getLogger(__name__)

DJANGO_SETTINGS_VAR = "DJANGO_SETTINGS_MODULE"
ENVIRONMENT_VARIABLE = "DATABASE_CONFIG_JSON"

DEFAULT_DJANGO_DB = "unspecified"
DEFAULT_ENGINE = "postgresql"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5432
DEFAULT_USER = "postgres"

SUPPORTED_CONFIG = {
    # Required database configuration
    "database-name": None,
    # Optional database configuration
    "database-engine": DEFAULT_ENGINE,
    "database-host": DEFAULT_HOST,
    "database-port": DEFAULT_PORT,
    "database-user": "postgres",
    "database-password": None,
    # Optionally specify configured Django database
    "django-database": DEFAULT_DJANGO_DB,
}


try:
    from django.utils.functional import empty
    EMPTY = empty
except ImportError:
    EMPTY = object()


class PgDatabaseSettings(object):

    _database_info = None
    _database_config = None
    _django_settings = None
    _django_database = None

    def __init__(self):
        self._init_database_config()
        self._init_django_settings()

    def _init_database_config(self):
        """ Initialize a valid database configuration if one has been provided """

        if self._database_config is not None:
            return

        self._database_config = {}

        if ENVIRONMENT_VARIABLE not in os.environ:
            logger.debug("Database configuration not provided")

        elif not (os.environ[ENVIRONMENT_VARIABLE] or "").endswith(".json"):
            config_path = os.environ[ENVIRONMENT_VARIABLE]
            raise EnvironmentError(f"Invalid database configuration file: {config_path}")

        else:
            try:
                config_path = os.environ[ENVIRONMENT_VARIABLE]
                config_file = pathlib.Path(config_path).expanduser().resolve(strict=True)
                with open(config_file) as config_json:
                    self._database_config = json.loads(config_json.read())

            except FileNotFoundError:
                raise EnvironmentError(f"Database configuration file does not exist: {config_path}")
            except ValueError:
                raise EnvironmentError(f"Database configuration file does not contain JSON: {config_path}")

            # Remove values from overridden configuration if not present in supported configuration
            self._database_config = {k: v for k, v in self._database_config.items() if k in SUPPORTED_CONFIG}
            # Assign defaults in supported configuration if not present in overridden configuration
            self._database_config.update({k: v for k, v in SUPPORTED_CONFIG.items() if k not in self._database_config})

    def _init_django_settings(self):
        """ Capture properly configured Django settings if Django is present and configured """

        if self._django_settings is not None:
            return

        self._django_settings = EMPTY

        try:
            from django.core.exceptions import ImproperlyConfigured

            try:
                from django.conf import settings as _django_settings

                # Raises ImproperlyConfigured without DJANGO_SETTINGS_MODULE
                getattr(_django_settings, 'DATABASES', None)
                self._django_settings = _django_settings

            except ImproperlyConfigured as ex:
                logger.debug(f"Django settings are not configured: {ex}")
        except ImportError as ex:
            logger.debug(f"Django is not configured: {ex}")

    def _init_database_info(self):
        """ Lazily initialize database info from configuration file or Django settings """

        if self._database_info is not None:
            return

        self._database_info = {}
        self._django_database = {}

        # Validate and apply database configuration

        # Reference DATABASES at run time to support django.test.override_settings
        django_databases = getattr(self._django_settings, "DATABASES", None)

        if not self._database_config and not django_databases:
            raise EnvironmentError(
                "No database configuration available:\n"
                f'Hint: have you set the "{ENVIRONMENT_VARIABLE}" environment variable?'
            )
        elif not django_databases:
            logger.debug("Using provided database configuration")

        elif not self._database_config or self._database_config.get("django-database") != DEFAULT_DJANGO_DB:
            logger.debug("Applying Django database configuration")

            django_database = self._database_config.get("django-database") or "default"
            django_database = dict(django_databases[django_database])

            # Update configuration with values from Django config or defaults
            self._database_config["database-engine"] = (django_database.get("ENGINE") or DEFAULT_ENGINE).split(".")[-1]
            self._database_config["database-host"] = django_database.get("HOST") or DEFAULT_HOST
            self._database_config["database-port"] = django_database.get("PORT") or DEFAULT_PORT
            self._database_config["database-name"] = django_database.get("NAME")
            self._database_config["database-user"] = django_database.get("USER")
            self._database_config["database-password"] = django_database.get("PASSWORD")

            self._django_database = django_database

        for database_key in ("database-name", "database-user"):
            if self._database_config[database_key] is None:
                raise EnvironmentError(f'Database configuration missing required key: "{database_key}"')

        self._database_info = {
            # Properties named after sqlalchemy.engine.url.URL params
            "database": self._database_config["database-name"],
            "drivername": self._database_config["database-engine"],
            "host": self._database_config["database-host"],
            "port": self._database_config["database-port"],
            "username": self._database_config["database-user"],
            "password": self._database_config["database-password"],
        }

        if self._database_info["password"] is None:
            self._database_info["host"] = None

    @property
    def database_info(self):
        if self._database_info is None:
            self._init_database_info()
        return dict(self._database_info)

    @property
    def django_database(self):
        if self._django_database is None:
            self._init_database_info()
        return dict(self._django_database)

    @property
    def database_engine(self):
        return (self._database_info or self.database_info)["drivername"]

    @property
    def database_host(self):
        return (self._database_info or self.database_info)["host"]

    @property
    def database_name(self):
        return (self._database_info or self.database_info)["database"]

    @property
    def database_port(self):
        return (self._database_info or self.database_info)["port"]

    @property
    def database_user(self):
        return (self._database_info or self.database_info)["username"]

    @property
    def database_password(self):
        return (self._database_info or self.database_info)["password"]


settings = PgDatabaseSettings()
