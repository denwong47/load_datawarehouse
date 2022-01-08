from load_datawarehouse.api import boto3

from load_datawarehouse.classes import  DataWarehouse, \
                                        DataWarehouseUnavailable

if (not isinstance(boto3, Exception)):
    class DataWarehouse_RedShift(DataWarehouse):
        pass
else:
    class DataWarehouse_RedShift(DataWarehouseUnavailable):
        exception = boto3