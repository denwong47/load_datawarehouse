# print ("load_datawarehouse main module loaded")
from load_datawarehouse.exceptions import WarehouseAPINotInstalled

class DataWarehouse():
    pass

class DataWarehouseUnavailable():
    exception = Exception("Generic Exception for unavailable API.")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        raise self.exception