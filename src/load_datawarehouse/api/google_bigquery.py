
from io import DEFAULT_BUFFER_SIZE
import os
from enum import Enum
from typing import List
from load_datawarehouse.exceptions import   WarehouseAPIFaked
from load_datawarehouse.classes import      APITypesMetaclass

class BigQueryAPIFaked(WarehouseAPIFaked):
    # class _pandas_helpers():
    #     pass
    pass

class BigQueryAPINotInstalled(BigQueryAPIFaked):
    pass

class BigQueryAPICredentialsMissing(BigQueryAPIFaked):
    pass


try:
    import google
    from google.cloud import bigquery

    class bigquery_types(metaclass = APITypesMetaclass):
        from google.cloud.bigquery.client import Client
        from google.cloud.bigquery.job import QueryJob, CopyJob, LoadJob, ExtractJob
        from google.cloud.bigquery.dataset import Dataset, DatasetListItem, DatasetReference, AccessEntry
        from google.cloud.bigquery.table import PartitionRange, RangePartitioning, Row, RowIterator, SnapshotDefinition, Table, TableListItem, TableReference, TimePartitioning, TimePartitioningType
        from google.cloud.bigquery.routine import DeterminismLevel, Routine, RoutineArgument, RoutineReference, RoutineType
        from google.cloud.bigquery.schema import SchemaField, PolicyTagList
        

    if (not "GOOGLE_APPLICATION_CREDENTIALS" in os.environ):
        google = BigQueryAPICredentialsMissing(f"Environment Variable 'GOOGLE_APPLICATION_CREDENTIALS' is not set; Google Applications cannot be used.")
        bigquery = BigQueryAPICredentialsMissing(f"Environment Variable 'GOOGLE_APPLICATION_CREDENTIALS' is not set; BigQuery API cannot be used.")


except (ModuleNotFoundError,
        ImportError
        ) as e:
    google = BigQueryAPINotInstalled(str(e))
    bigquery = BigQueryAPINotInstalled(str(e))

    # bigquery_types is only used to get past type hinting when google.cloud API is not actually installed.
    class bigquery_types(metaclass = APITypesMetaclass):
        pass

