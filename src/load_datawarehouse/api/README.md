# load_datawarehouse.api
`api` module handles importing of the SaaS's default libraries, e.g. `google.cloud`, `boto3` etc.

It is strongly recommended that all references to those libraries be routed through `api` module; for example:
```
from google.cloud import bigquery

_client = bigquery.Client()
```
should be changed to
```diff
-from google.cloud import bigquery
+from load_datawarehouse.api import bigquery

_client = bigquery.Client()
```

While `load_datawarehouse.api` references the same `bigquery` instance without any wrappers, it does not throw an exception if `google.cloud` is not installed, but instead return an instance of an `Exception` describing the error occured. This simplifies the testing of module availability by not having to try-catch every time bigquery is referenced - one only need to check if `bigquery` is an instance of `Exception`, or more specifically, `load_datawarehouse.exceptions.WarehouseAPIFaked`.

Please note that these `Exception` instances will raise itself if any attributes are requested.


Additionally, to allow for type hinting without the modules loaded, `api` module provides a class for each SaaS platform. For example:
```
from google.cloud import bigquery

def some_func(
    table:bigquery.table.Table,
):
    pass
```
will cause an exception if `google.cloud` is not installed; however

```diff
!from load_dataawarehouse import bigquery_types

def some_func(
-    table:bigquery.table.Table,
+    table:bigquery_types.Table,
)
```
will not. This is because `bigquery_types` has a built in __getattr__ function that returns type(None) when any undefined attributes are requested, allowing type hinting checks to pass.