from load_datawarehouse.exceptions import WarehouseAPINotInstalled

try:
    import boto3
except (ModuleNotFoundError,
        ImportError
        ) as e:
    boto3 = WarehouseAPINotInstalled(str(e))