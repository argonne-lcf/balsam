import os
import importlib.util

from balsam.client.client import ClientAPI
__all__ = ['conf']

class SiteConfiguration:

    def __init__(self):
        self._site_path = None
        self._settings_module = None
        self._client = None

    @property
    def site_path(self):
        if self._site_path is not None:
            return self._site_path
        path = os.environ.get('BALSAM_SITE_PATH')
        if not path:
            raise RuntimeError(f'Set BALSAM_SITE_PATH to a site directory')
        self._site_path = path
        return self._site_path

    @property
    def settings_module(self):
        if self._settings_module is not None:
            return self._settings_module
        site_path = self.site_path
        settings_path = os.path.join(site_path, 'settings.py')
        if not os.path.isfile(settings_path):
            if not os.path.isdir(site_path):
                raise RuntimeError(f'{site_path} is not a directory')
            else:
                raise RuntimeError(f"{settings_path} must exist")

        try:
            module = self._load_module(settings_path)
            self._settings_module = module
        except Exception as e:
            raise RuntimeError(
            f"An Exception occured while loading {settings_path}:" 
            "Please fix the error in this module in order to proceed."
            ) from e
        return self._settings_module

    @property
    def apps_path(self):
        return os.path.join(self.site_path, 'apps')
    
    @property
    def log_path(self):
        return os.path.join(self.site_path, 'log')
    
    @property
    def job_path(self):
        return os.path.join(self.site_path, 'qsubmit')
    
    @property
    def data_path(self):
        return os.path.join(self.site_path, 'data')

    @property
    def client(self):
        if self._client is not None:
            return self._client
        self._client = self._fetch_from_settings("client", ClientAPI)
        return self._client

    def _load_module(self, file_path):
        spec = importlib.util.spec_from_file_location(
            file_path, file_path
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _fetch_from_settings(self, name, cls):
        settings = self.settings_module
        obj = getattr(settings, name)
        if isinstance(obj, cls) or issubclass(obj, cls):
            return obj
        if callable(obj):
            obj = obj()
        if not (isinstance(obj, cls) or issubclass(obj, cls)):
            raise TypeError(
                f"{name} in site settings must be an instance of {cls},"
                f" or a callable that returns {cls}.  It is currently "
                f"{obj} ({type(obj)}). Please fix."
            )
        return obj

conf = SiteConfiguration()
