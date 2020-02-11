from .client import ClientAPI


class RESTClient(ClientAPI):
    pass


ClientAPI._class_registry["RESTClient"] = RESTClient
