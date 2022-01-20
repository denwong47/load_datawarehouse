from load_datawarehouse.classes import APITypesMetaclass
from load_datawarehouse.exceptions import WarehouseAPINotInstalled, WarehouseAPIFaked


class RedshiftAPIFaked(WarehouseAPIFaked):
    class _pandas_helpers():
        pass

class RedshiftAPINotInstalled(RedshiftAPIFaked):
    pass

class RedshiftAPICredentialsMissing(RedshiftAPIFaked):
    pass


try:
    import boto3
    
    # The intention of redshift_types is to allow for type hinting even if boto3 is not installed/loaded.
    # However this is likely to be empty due to the way boto3 dynamically create classes with type()...
    class redshift_types(metaclass = APITypesMetaclass):
        pass

except (ModuleNotFoundError,
        ImportError
        ) as e:
    boto3 = RedshiftAPINotInstalled(str(e))

    class redshift_types(metaclass = APITypesMetaclass):
        pass