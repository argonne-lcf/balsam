from pathlib import Path
from .client import ClientAPI
from .orm_client import DjangoORMClient


class PostgresDjangoORMClient(DjangoORMClient):
    def __init__(
        self,
        user,
        password,
        site_id,
        site_path,
        host,
        port,
        rel_db_path="balsamdb",
        auto_port=True,
        db_name="balsam",
        engine="django.db.backends.postgresql",
        conn_max_age=60,
        db_options={
            "connect_timeout": 30,
            "client_encoding": "UTF8",
            "default_transaction_isolation": "read committed",
            "timezone": "UTC",
        },
    ):
        """
        Initialize Django with a settings module for ORM usage only
        """
        from balsam.server.conf import db_only

        db_only.set_database(
            user, password, host, port, db_name, engine, conn_max_age, db_options
        )
        self.user = user
        self.password = password
        self.site_path = site_path
        self.site_id = site_id
        self.host = host
        self.port = port
        self.db_name = db_name
        self.engine = engine
        self.conn_max_age = conn_max_age
        self.db_options = db_options
        self.rel_db_path = rel_db_path
        self.auto_port = auto_port

    def dict_config(self):
        d = {
            "user": self.user,
            "password": self.password,
            "site_id": self.site_id,
            "host": self.host,
            "port": self.port,
            "db_name": self.db_name,
            "engine": self.engine,
            "conn_max_age": self.conn_max_age,
            "db_options": self.db_options,
            "rel_db_path": self.rel_db_path,
            "auto_port": self.auto_port,
        }
        return d

    def establish_connection(self):
        """
        Restart DB server
        """
        from balsam.server.conf import db_only
        from balsam.util import postgres as pg

        if not self.rel_db_path:
            raise RuntimeError(
                f"Do not have a filesystem path to control DB "
                f"at {self.host}:{self.port}.\n"
                f"Please ask owner to restart the DB, then update the "
                f"host and port in {self.INFO_FILENAME}"
            )
        db_path = Path(self.site_path).joinpath(self.rel_db_path)
        if self.auto_port:
            host, port = pg.identify_hostport()
            self.host, self.port = host, port
            pg.mutate_conf_port(db_path, port)
            db_only.set_database(
                self.user,
                self.password,
                host,
                port,
                self.db_name,
                self.engine,
                self.conn_max_age,
                self.db_options,
            )
            self.dump_yaml()
        pg.start_db(db_path)


ClientAPI._class_registry["PostgresDjangoORMClient"] = PostgresDjangoORMClient
