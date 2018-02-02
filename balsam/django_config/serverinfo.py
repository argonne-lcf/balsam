import json
import os
import socket

ADDRESS_FNAME = 'dbwriter_address'

class ServerInfo:
    def __init__(self, balsam_db_path):
        self.path = os.path.join(balsam_db_path, ADDRESS_FNAME)
        self.data = {}

        if not os.path.exists(self.path):
            self.update(self.data)
        else:
            self.refresh()

        if not self.data.get('balsamdb_path'):
            self.update({'balsamdb_path': balsam_db_path})

        if self.data.get('address') and os.environ.get('IS_SERVER_DAEMON')=='True':
            raise RuntimeError(f"A running server address is already posted at {self.path}\n"
                               '  Use "balsam dbserver --stop" to shut it down.\n'
                               '  If you are sure there is no running server process, the'
                               ' daemon did not have a clean shutdown.\n  Use "balsam'
                               ' dbserver --reset <balsam_db_directory>" to reset the server file'
                              )

    def get_free_port_and_address(self):
        hostname = socket.gethostname()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', 0))
        port = int(sock.getsockname()[1])
        sock.close()

        address = f'tcp://{hostname}:{port}'
        return address

    def get_free_port(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', 0))
        port = int(sock.getsockname()[1])
        sock.close()
        return port

    def get_sqlite3_info(self):
        new_address = self.get_free_port_and_address()
        info = dict(db_type='sqlite3', address=new_address)
        return info

    def get_postgres_info(self):
        hostname = socket.gethostname()
        port = self.get_free_port()
        info = dict(host=hostname, port=port)
        return info

    def reset_server_address(self):
        db = self['db_type']
        info = getattr(self, f'get_{db}_info')()
        self.update(info)
    
    def update(self, update_dict):
        self.refresh()
        self.data.update(update_dict)
        with open(self.path, 'w') as fp:
            fp.write(json.dumps(self.data))

    def get(self, key, default=None):
        if key in self.data:
            return self.data[key]
        else:
            return default

    def refresh(self):
        if not os.path.exists(self.path): return
        with open(self.path, 'r') as fp:
            self.data = json.loads(fp.read())

    def __getitem__(self, key):
        if self.data is None: self.refresh()
        return self.data[key]

    def __setitem__(self, key, value):
        self.update({key:value})
