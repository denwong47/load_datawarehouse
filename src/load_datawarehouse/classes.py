# print ("load_datawarehouse main module loaded")
from load_datawarehouse.exceptions import WarehouseAPINotInstalled

class DataWarehouse():
    pass

class DataWarehouseUnavailable():
    exception = Exception("Generic Exception for unavailable API.")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        raise self.exception

# metaclass that allows api_types to pass all type hint checks even if API is missing
class APITypesMetaclass(type):
    def __getattr__(self, attr):
        return type(None)