import json
import os
import subprocess
import socket
from getpass import getuser

ADDRESS_FNAME = 'server-info'


class ServerInfo:
    def __init__(self, balsam_db_path):
        self.balsam_db_path = os.path.abspath(os.path.expanduser(balsam_db_path))
        self.pg_db_path = os.path.join(self.balsam_db_path, 'balsamdb')
        self.path = os.path.join(self.balsam_db_path, ADDRESS_FNAME)
        self.data = {}
        if not os.path.exists(self.path):
            self._is_owner = True
            self.update(self.data)
        else:
            self._is_owner = os.stat(self.path).st_uid == os.getuid()
            self.refresh()

    def reset_server_address(self):
        self._ownership_check()
        info = self._conn_info()
        self.update(info)
        self._update_postgres_config()

    def update(self, update_dict):
        if os.path.exists(self.path): self.refresh()
        self.data.update(update_dict)
        with open(self.path, 'w') as fp:
            fp.write(json.dumps(self.data))


    def django_db_config(self):
        ENGINE = 'django.db.backends.postgresql_psycopg2'
        NAME = 'balsam'
        OPTIONS = {'connect_timeout' : 30}

        user = os.environ.get('BALSAM_USER', getuser())
        password = self.get('password', '')
        host = self.get('host', '')
        port = self.get('port', '')


        db = dict(ENGINE=ENGINE, NAME=NAME,
                  OPTIONS=OPTIONS, USER=user, PASSWORD=password,
                  HOST=host, PORT=port, CONN_MAX_AGE=60)

        DATABASES = {'default':db}
        return DATABASES

    def get(self, key, default=None):
        if key in self.data:
            return self.data[key]
        else:
            return default

    def refresh(self):
        with open(self.path, 'r') as fp:
            file_data = json.loads(fp.read())
        self.data.update(file_data)

    def __getitem__(self, key):
        return self.data[key]

    def __setitem__(self, key, value):
        self.update({key:value})
    
    def _ownership_check(self):
        if not self._is_owner:
            raise PermissionError(f"User {getuser()} does not own the server-info file for this BalsamDB;"
                                  " will not proceed")

    def _free_port(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', 0))
        port = int(sock.getsockname()[1])
        sock.close()
        return port

    def _conn_info(self):
        hostname = socket.gethostname()
        port = self._free_port()
        info = dict(host=hostname, port=port)
        return info

    def _update_postgres_config(self):
        self._ownership_check()
        conf_path = os.path.join(self.pg_db_path, 'postgresql.conf')
        config = open(conf_path).read()

        with open(f"{conf_path}.new", 'w') as fp:
            for line in config.split('\n'):
                if line.startswith('port'):
                    port_line = f"port={self['port']} # auto-set by balsam db\n"
                    fp.write(port_line)
                else:
                    fp.write(line + "\n")
        os.rename(f"{conf_path}.new", conf_path)
