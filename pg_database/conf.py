import json
import logging
import os
import pathlib

from frozendict import frozendict

logger = logging.getLogger(__name__)

DJANGO_SETTINGS_VAR = "DJANGO_SETTINGS_MODULE"
ENVIRONMENT_VARIABLE = "DATABASE_CONFIG_JSON"

DEFAULT_DJANGO_DB = "unspecified"
DEFAULT_ENGINE = "postgresql"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5432
DEFAULT_USER = "postgres"

DEFAULT_DATE_FORMAT = "%Y-%m-%d"
DEFAULT_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

DATABASE_PROPS = frozenset({
    # Supported database info properties
    "database_engine", "engine", "drivername",
    "database_name", "name", "database",
    "database_port", "port",
    "database_host", "host",
    "database_user", "user", "username",
    "database_password", "password",
    # Supported database query properties
    "connect_args", "pooling_args",
    # Supported django database properties
    "ENGINE", "django_engine",
    "NAME", "django_name",
    "PORT", "django_port",
    "HOST", "django_host",
    "USER", "django_user",
    "PASSWORD", "django_password",
    "OPTIONS", "django_options",
})
SUPPORTED_CONFIG = frozendict({
    # Required database configuration
    "database-name": None,
    # Optional database configuration
    "database-engine": DEFAULT_ENGINE,
    "database-host": DEFAULT_HOST,
    "database-port": DEFAULT_PORT,
    "database-user": "postgres",
    "database-password": None,
    # Optionally specify configured Django database
    "django-db-key": DEFAULT_DJANGO_DB,
    # Other non-required options:
    "connect-args": None,
    "pooling-args": None,
    "date-format": DEFAULT_DATE_FORMAT,
    "timestamp-format": DEFAULT_TIMESTAMP_FORMAT,
})


try:
    from django.utils.functional import empty
    EMPTY = empty
    DJANGO_INSTALLED = True
except ImportError:
    EMPTY = object()
    DJANGO_INSTALLED = False


class PgDatabaseSettings(object):
    """
    A class to consolidate settings derived from the following sources:
        * A JSON file identified by the DATABASE_CONFIG_JSON environment variable
        * An appropriately configured Django settings file if Django is installed

    Precedence of settings duplicated in both sources is as follows:
        * If no DATABASE_CONFIG_JSON file is specified, use Django.DATABASES
        * If DATABASE_CONFIG_JSON specifies a django-db-key, then use that Django database
        * Otherwise use the keys defined in DATABASE_CONFIG_JSON

    This is how database connection info is mapped between sources:
        | config key        | django   | database_info | settings property | default values |
        |:------------------|:---------|:--------------|:------------------|:---------------|
        | database-engine   | ENGINE   | drivername    | database_engine   | "postgresql"   |
        | database-host     | HOST     | host          | database_host     | "127.0.0.1"    |
        | database-port     | PORT     | port          | database_port     | 5432           |
        | database-name     | NAME     | database      | database_name     | <required>     |
        | database-user     | USER     | username      | database_user     | "postgres"     |
        | database-password | PASSWORD | password      | database_password | None           |

    This is how other database options are mapped:
        | config key       | django   | settings property | default values        |
        |:-----------------|:---------|:------------------|:----------------------|
        | connect-args     | OPTIONS  | connect_args      | None                  |
        | pooling-args     | None     | pooling_args      | <see sqlalchemy docs> |
        | django-db-key    | None     | django_db_key     | "default"             |
        | date-format      | None     | date_format       | "%Y-%m-%d"            |
        | timestamp-format | None     | timestamp_format  | "%Y-%m-%d %H:%M:%S"   |

    Of the above, only database-name and database-user are required.
    The others either have defaults or are not required for database connection.
    """

    empty = EMPTY

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

        self._database_config = frozendict()

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
                    database_config = json.loads(config_json.read())

            except FileNotFoundError:
                raise EnvironmentError(f"Database configuration file does not exist: {config_path}")
            except ValueError:
                raise EnvironmentError(f"Database configuration file does not contain JSON: {config_path}")

            # Remove values from overridden configuration if not present in supported configuration
            database_config = {k: v for k, v in database_config.items() if k in SUPPORTED_CONFIG}
            # Assign defaults in supported configuration if not present in overridden configuration
            database_config.update({k: v for k, v in SUPPORTED_CONFIG.items() if k not in database_config})

            self._database_config = frozendict(database_config)

    def _init_django_settings(self):
        """ Capture properly configured Django settings if Django is present and configured """

        if self._django_settings is not None:
            return

        self._django_settings = EMPTY

        if DJANGO_INSTALLED:
            try:
                from django.core.exceptions import ImproperlyConfigured
                from django.conf import settings as _django_settings

                # Raises ImproperlyConfigured without DJANGO_SETTINGS_MODULE
                getattr(_django_settings, 'DATABASES', None)
                self._django_settings = _django_settings

            except ImproperlyConfigured as ex:
                logger.debug(f"Django settings are not configured: {ex}")

    def _init_database_info(self):
        """
        Lazily initializes database info from configuration file or Django settings.
        The implementation is lazy in that database info isn't determined at instantiation time.
        """

        if self._database_info is not None:
            return

        self._database_info = frozendict()
        self._django_database = frozendict()

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

        elif not self._database_config or self._database_config.get("django-db-key") != DEFAULT_DJANGO_DB:
            logger.debug("Applying Django database configuration")

            django_db_key = self._database_config.get("django-db-key") or "default"
            if django_db_key not in django_databases:
                raise EnvironmentError(f'No Django database configured for: "{django_db_key}"')

            django_database = {}.fromkeys(("ENGINE", "HOST", "PORT", "NAME", "USER", "PASSWORD"))
            django_database.update(django_databases[django_db_key])

            # Update configuration with values from Django config or defaults
            database_config = dict(self._database_config)
            database_config.update({
                "database-engine": DEFAULT_ENGINE,
                "database-host": django_database.get("HOST") or DEFAULT_HOST,
                "database-port": django_database.get("PORT") or DEFAULT_PORT,
                "database-name": django_database.get("NAME"),
                "database-user": django_database.get("USER"),
                "database-password": django_database.get("PASSWORD"),
                "connect-args": django_database.get("OPTIONS", database_config.get("connect-args"))
            })

            self._database_config = frozendict(database_config)
            self._django_database = frozendict({f"django_{k}".lower(): v for k, v in django_database.items()})

        for database_key in ("database-name", "database-user"):
            if self._database_config[database_key] is None:
                raise EnvironmentError(f'Database configuration missing required key: "{database_key}"')

        database_info = {
            # Properties named after sqlalchemy.engine.url.URL params
            "database": self._database_config["database-name"],
            "drivername": self._database_config["database-engine"],
            "host": self._database_config["database-host"],
            "port": self._database_config["database-port"],
            "username": self._database_config["database-user"],
            "password": self._database_config["database-password"],
        }

        if database_info["password"] is None:
            database_info["host"] = None

        self._database_info = frozendict(database_info)

    @property
    def database_info(self):
        if self._database_info is None:
            self._init_database_info()
        return self._database_info

    @property
    def django_database(self):
        if self._django_database is None:
            self._init_database_info()
        return self._django_database

    @property
    def database_engine(self):
        """ Routes "database_engine" to database info property: drivername """
        return (self._database_info or self.database_info)["drivername"]

    @property
    def database_name(self):
        """ Routes "database_name" to database info property: database """
        return (self._database_info or self.database_info)["database"]

    @property
    def database_user(self):
        """ Routes "database_user" to database info property: username """
        return (self._database_info or self.database_info)["username"]

    def __getattr__(self, name):
        """ Exposes database info and config consistently with precedence  """

        if self._database_info is None and name in DATABASE_PROPS:
            self._init_database_info()

        # First check database info initialized from config or django

        if self._database_info:
            if name in self._database_info:
                return (self._database_info or self.database_info)[name]

            if name in ("engine", "name", "user"):
                return getattr(self, f"database_{name}")

            if name.startswith("database_"):
                prop = name[len("database_"):]
                if prop in self._database_info:
                    return (self._database_info or self.database_info)[prop]

        # Next check for references to django database (i.e. ENGINE, django_engine)

        if self._django_database:
            if name in self._django_database:
                return (self._django_database or self.django_database)[name]

            if name.isupper():
                prop = f"django_{name.lower()}"
                if prop in self._django_database:
                    return (self._django_database or self.django_database)[prop]

        # Finally check for any other configured properties (i.e. connect_args, date_format, Etc.)

        if "-" not in name:
            prop = name.replace("_", "-")
            if prop in self._database_config:
                return self._database_config[prop]

        return None


settings = PgDatabaseSettings()
