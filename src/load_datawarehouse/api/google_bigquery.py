
from io import DEFAULT_BUFFER_SIZE
import os
from enum import Enum
from typing import List
from load_datawarehouse.exceptions import   WarehouseAPIFaked

class BigQueryAPIFaked(WarehouseAPIFaked):
    class schema():
        class SchemaField():
            pass

    class table():
        class Table():
            pass
        
    class _pandas_helpers():
        pass


class BigQueryAPINotInstalled(BigQueryAPIFaked):
    pass

class BigQueryAPICredentialsMissing(BigQueryAPIFaked):
    pass

try:
    import google
    from google.cloud import bigquery

    if (not "GOOGLE_APPLICATION_CREDENTIALS" in os.environ):
        google = BigQueryAPICredentialsMissing(f"Environment Variable 'GOOGLE_APPLICATION_CREDENTIALS' is not set; Google Applications cannot be used.")
        bigquery = BigQueryAPICredentialsMissing(f"Environment Variable 'GOOGLE_APPLICATION_CREDENTIALS' is not set; BigQuery API cannot be used.")

except (ModuleNotFoundError,
        ImportError
        ) as e:
    google = BigQueryAPINotInstalled(str(e))
    bigquery = BigQueryAPINotInstalled(str(e))

