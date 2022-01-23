# print ("bigquery Submodule loaded")
from datetime import datetime
import enum
from typing import Any, Union, Iterable, Mapping, Tuple, Dict, Sequence
from collections import OrderedDict
from warnings import warn
from load_datawarehouse.schema import is_records

import pandas as pd

from load_datawarehouse.api import google, bigquery

from load_datawarehouse.classes import  DataWarehouse, \
                                        DataWarehouseUnavailable, \
                                        QuerySort
from load_datawarehouse.exceptions import   WarehouseAPINotInstalled, \
                                            WarehouseInvalidInput, \
                                            WarehouseAccessDenied, \
                                            WarehouseTableNotFound, \
                                            WarehouseTableGenericError, \
                                            WarehouseTableRowsInvalid

import load_datawarehouse.data
from load_datawarehouse.bigquery.config import BIGQUERY_JSON_BYTES_LIMIT, BIGQUERY_DEFAULT_LOCATION
import load_datawarehouse.bigquery.schema

try:
    from dict_tree import DictionaryTree
except (ImportError, ModuleNotFoundError) as e:
    class DictionaryTree():
        pass

class locations(enum.Enum):
    DEFAULT                 =None
    US_CENTRAL1	            ="us-central1"
    US_WEST4	            ="us-west4"
    US_WEST2	            ="us-west2"
    NORTHAMERICA_NORTHEAST1	="northamerica-northeast1"
    US_EAST4	            ="us-east4"
    US_WEST1	            ="us-west1"
    US_WEST3	            ="us-west3"
    SOUTHAMERICA_EAST1	    ="southamerica-east1"
    SOUTHAMERICA_WEST1	    ="southamerica-west1"
    US_EAST1	            ="us-east1"
    NORTHAMERICA_NORTHEAST2	="northamerica-northeast2"
    EUROPE_WEST1	        ="europe-west1"
    EUROPE_NORTH1	        ="europe-north1"
    EUROPE_WEST3	        ="europe-west3"
    EUROPE_WEST2	        ="europe-west2"
    EUROPE_WEST4	        ="europe-west4"
    EUROPE_CENTRAL2	        ="europe-central2"
    EUROPE_WEST6	        ="europe-west6"
    ASIA_SOUTH2	            ="asia-south2"
    ASIA_EAST2	            ="asia-east2"
    ASIA_SOUTHEAST2	        ="asia-southeast2"
    AUSTRALIA_SOUTHEAST2	="australia-southeast2"
    ASIA_SOUTH1	            ="asia-south1"
    ASIA_NORTHEAST2	        ="asia-northeast2"
    ASIA_NORTHEAST3	        ="asia-northeast3"
    ASIA_SOUTHEAST1	        ="asia-southeast1"
    AUSTRALIA_SOUTHEAST1	="australia-southeast1"
    ASIA_EAST1	            ="asia-east1"
    ASIA_NORTHEAST1	        ="asia-northeast1"

#=====================================================================================================================================================================
# google.cloud is installed

if (not isinstance(google, Exception) and \
    not isinstance(bigquery, Exception)):

    def get_bigquery_client()->bigquery.client.Client:
        try:
            return bigquery.Client()
        except google.auth.exceptions.DefaultCredentialsError as e:
            raise WarehouseAccessDenied(
                str(e)
            )
        except Exception as e:
            raise WarehouseTableGenericError(
                f"Exception occured during creation of API client: {str(e)}",
                exception = e,
            )

    def list_bigquery_projects(
        client:bigquery.client.Client,
        *args,
        **kwargs,
    ):
        return client.list_projects(*args, **kwargs,)

    def is_online(
        client:bigquery.client.Client,
    ):
        try:
            list_bigquery_projects(client)
            return True
        except Exception as e:
            return WarehouseTableGenericError(
                f"Exception occured during client connection: {str(e)}",
                exception = e,
            )

    def select_bigquery_table(
        table: Union[
            bigquery.table.Table,
            bigquery.table.TableReference,
            bigquery.table.TableListItem,
            str
        ],
        **kwargs,
    )->bigquery.table.Table:
        """
        Locally select a BigQuery table, returning a Table object.
    
        Does not contact cloud servers - it just creates a Table reference, regardless of whether it exists in the cloud or not.
        """
        return bigquery.table.Table(
            table
        )

    def get_bigquery_table(
        client:bigquery.client.Client,
        table: Union[
            bigquery.table.Table,
            bigquery.table.TableReference,
            bigquery.table.TableListItem,
            str
        ],
        **kwargs,
    )->bigquery.table.Table:
        """
        Get a BigQuery table on the cloud, returning a Table object.
    
        Raises exceptions should the cloud report any errors, such as the table not existing.
        """

        try:
            _bq_table = client.get_table(
                table=table,
                **kwargs,
            )
        except google.api_core.exceptions.NotFound as e:
            _bq_table = WarehouseTableNotFound(f"{table} not found on bigquery.")
        except Exception as e:
            _bq_table = WarehouseTableGenericError(
                f"Exception occured during table fetching: {str(e)}",
                exception = e,
            )
        
        return _bq_table

    def create_bigquery_table(
        client:bigquery.client.Client,
        table: Union[
            bigquery.table.Table,
            bigquery.table.TableReference,
            bigquery.table.TableListItem,
            str
        ],
        replace:bool=True,
        schema:Iterable[
            Union[
                Dict[str, Any],
                bigquery.schema.SchemaField,
            ],
        ]=None,
        expires:Union[
            datetime,
            None,
        ]=None,
        # location:Union[
        #     locations,
        #     str,
        # ]=locations(BIGQUERY_DEFAULT_LOCATION), # location is at dataset level - TODO
        **kwargs,
    ):
        """
        Creates a BigQuery table on the cloud, returning a Table object.
        """

        if (replace):
            # If schema is not provided, try to get the existing one
            if (schema is None):
                _existing_table = get_bigquery_table(client, table)
                if (_existing_table):
                    schema = _existing_table.schema
                else:
                    return WarehouseTableNotFound("Cannot create new table with no schema. Please provide a schema.")
                    
            # Drop table if already exist, ignore error if not
            _new_table = drop_bigquery_table(
                client,
                table,
                not_found_ok=True,
                # **kwargs
            )
        else:
            _new_table = False

        # Replace schema if needed
        if (isinstance(table, bigquery.table.TableReference) or \
            isinstance(table, str) or \
            _new_table):
            table = bigquery.table.Table(
                table_ref=table,
                schema=schema,
            )

        if (isinstance(table, Exception)):
            return table

        try:
            _bq_table = client.create_table(
                table=table,
                **kwargs,
            )

            if (not set_expiry_bigquery_table(client, _bq_table, expires)):
                warn(
                    RuntimeWarning(f"WARNING: Table expiry was not set correctly; expiry time likely incorrect.")
                )
        except google.api_core.exceptions.Conflict as e:
            _bq_table = WarehouseTableGenericError(f"{table} already exists: {str(e)}")
        except Exception as e:
            _bq_table = WarehouseTableGenericError(
                f"Exception occured during table creation: {str(e)}",
                exception = e,
            )
        
        return _bq_table

    def apply_changes_bigquery_table(
        client:bigquery.client.Client,
        table: Union[
            bigquery.table.Table,
            bigquery.table.TableReference,
            bigquery.table.TableListItem,
            str
        ],
    ):
        """
        Apply local changes on Table to the cloud.
        """

        table = client.update_table(table, ["expires"])
        return table

    def set_expiry_bigquery_table(
        client:bigquery.client.Client,
        table: Union[
            bigquery.table.Table,
            bigquery.table.TableReference,
            bigquery.table.TableListItem,
            str
        ],
        expires:Union[
            datetime,
            None,
        ]=None,
        update:bool=True,
    ):
        """
        Set expiry time of Table.

        Parameters:
        - update           If True, immediately apply changes to cloud.
        """

        table = get_bigquery_table(client, table)

        if (not isinstance(table, Exception)):
            try:
                table.expires = expires

                if (update):
                    table = apply_changes_bigquery_table(client, table)
                
            except ValueError as e:
                return WarehouseInvalidInput(str(e))

            return table
        else:
            # Return the exception
            return table

    def set_schema_bigquery_table(
        client:bigquery.client.Client,
        table: Union[
            bigquery.table.Table,
            bigquery.table.TableReference,
            bigquery.table.TableListItem,
            str
        ],
        schema: Union[
            Iterable[
                Union[
                    bigquery.schema.SchemaField,
                    Dict[str, str],
                ],
            ],
            tuple,
        ]=[],
        update:bool=True,
    ):
        """
        Set schema of Table.

        Parameters:
        - schema            Can be SchemaField or api_repr.
        - update            If True, immediately apply changes to cloud.
        """

        if (is_records(schema)):
            schema = load_datawarehouse.bigquery.schema.get_api_repr_from_record_fields(schema, None)
        
        try:
            table.schema = schema

            if (update):
                table = apply_changes_bigquery_table(client, table)

            return table
        except Exception as e: # The offical API guide only says "Exception" - no subtype: https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.Table.html#google.cloud.bigquery.table.Table.schema
            return WarehouseInvalidInput(str(e))
        

    def drop_bigquery_table(
        client:bigquery.client.Client,
        table: Union[
            bigquery.table.Table,
            bigquery.table.TableReference,
            bigquery.table.TableListItem,
            str
        ],
        not_found_ok:bool=True,
        **kwargs,
    )->bool:
        """
        Drop a BigQuery Table.

        Parameters:
        - not_found_ok          If True, ignore non-existing Tables. Otherwise, raise WarehouseTableNotFound.
        """

        kwargs["not_found_ok"] = kwargs.get("not_found_ok", True)

        try:
            client.delete_table(
                table,
                **kwargs,
            )
            _return = True
        except google.api_core.exceptions.NotFound as e:
            _return = not_found_ok or WarehouseTableNotFound(f"{table} not found on bigquery.")
        except Exception as e:
            _return = WarehouseTableGenericError(
                f"Exception occured during table dropping: {str(e)}",
                exception = e,
            )

        return _return

    def load_bigquery_table(
        client:bigquery.client.Client,
        table: Union[
            bigquery.table.Table,
            bigquery.table.TableReference,
            bigquery.table.TableListItem,
            str
        ],
        data:Union[
            Iterable[Dict], # records
            pd.DataFrame,   # DataFrame
        ],
        schema:Iterable[
            Union[
                Dict[str,str],
                bigquery.schema.SchemaField,
            ],
        ]=None,
        full_schema:bool=False,
        **kwargs,
    )->Sequence:
        """
        Load data into a BigQuery Table.

        Parameters:
        - schema            Can be SchemaField or api_repr. If None, schema will be automatically generated from data values.
        - full_schema       If True, do not attempt to generate schema.

        This currently uses streaming to upload data, which is quite expensive.

        TODO allow for batch loading as a parameter.
        """

        # Prepare data - sort out invalid keys and stuff
        data = load_datawarehouse.data.prepare(data)

        # Look for table
        table_obj = get_bigquery_table(client=client, table=table)
        _table_exists = not (isinstance(
            table_obj,
            WarehouseTableNotFound,
        ))

        # print (f"Table {table} {'' if _table_exists else 'DOES NOT '} exists.")

        # Get schema from table if exists
        if (_table_exists and not schema):
            schema = table_obj.schema

        # Create our own schema if schema provided is not full
        if (not full_schema):
            schema = load_datawarehouse.bigquery.schema.extract(
                obj = data,
                schema = schema,
            )

        # print (schema)

        if (not _table_exists):
            # Create table if not present
            table = create_bigquery_table(
                client=client,
                table=table,
                schema=schema,
            )
        else:
            table = table_obj

        try:
            for _chunk in load_datawarehouse.data.chunks(
                data,
                size_limit=BIGQUERY_JSON_BYTES_LIMIT,
                max_iteration=6
                ):

                client.insert_rows(
                    table,
                    rows=_chunk,
                    **kwargs,
                )

            _return = True
        except ValueError as e:
            if ("determine schema" in str(e)):
                _return = WarehouseTableRowsInvalid(f"Data schema cannot be determined. Pass schema fields to selected_fields.")
            else:
                _return = WarehouseTableRowsInvalid(f"rows of type({type(data)}) cannot be loaded. Expected iterable of Dicts or Tuples.")
        except google.api_core.exceptions.Forbidden as e:
            _return = WarehouseAccessDenied(
                f"Access denied for user: {str(e)}",
                exception = e,
            )
        except Exception as e:
            _return = WarehouseTableGenericError(
                f"Exception occured during table loading: {str(e)}",
                exception = e,
            )

        return _return

    def query_bigquery(
        client:bigquery.client.Client,
        query:str,   
    ):
        """
        Send a well formed SQL Query string to the cloud.

        TODO WIP
        """
        pass

    def fetch_bigquery_table(
        client:bigquery.client.Client,
        table: Union[
            bigquery.table.Table,
            bigquery.table.TableReference,
            bigquery.table.TableListItem,
            str
        ],
        fields:Union[
            Iterable[str],
            str,
        ]="*",
        sort:Union[
            Iterable[
                Tuple[str, QuerySort],
            ],
            None,
        ]=(),
        count:int=10,
    ):
        """
        Fetch data from a table by specifying attributes.

        TODO WIP
        """
        pass
        
    #=====================================================================================================================================================================

    class DataWarehouse_BigQuery(DataWarehouse):
        """
        BigQuery DataWarehouse subclass.

        This equates to one Table on BigQuery.
        """

        bqtable = None

        def __init__(
            self,
            table: bigquery.table.Table,
            **kwargs,
        ):
            self.bqtable = table

        @classmethod
        def get(
            cls,
            table: Union[
                bigquery.table.Table,
                bigquery.table.TableReference,
                bigquery.table.TableListItem,
                str
            ],
            **kwargs,
        ):
            """
            Get a BigQuery table on the cloud, returning a DataWarehouse_BigQuery object.
    
            Raises exceptions should the cloud report any errors, such as the table not existing.
            """

            _table = get_bigquery_table(
                client = get_bigquery_client(),
                table = table,
            )

            if (isinstance(_table, Exception)):
                raise _table
            else:
                return cls(_table)
        
        @classmethod
        def select(
            cls,
            table: Union[
                bigquery.table.Table,
                bigquery.table.TableReference,
                bigquery.table.TableListItem,
                str
            ],
            **kwargs,
        ):
            """
            Locally select a BigQuery table, returning a DataWarehouse_BigQuery object.
    
            Does not contact cloud servers - it just creates a Table reference, regardless of whether it exists in the cloud or not.
            """

            _table = select_bigquery_table(table)

            return cls(_table)

        @classmethod
        def new(
            cls,
            table: Union[
                bigquery.table.Table,
                bigquery.table.TableReference,
                bigquery.table.TableListItem,
                str
            ],
            replace:bool=False, # Default to NOT replace existing just in case
            schema:Iterable[
                Union[
                    Dict[str, Any],
                    bigquery.schema.SchemaField,
                ],
            ]=None,
            expires:Union[
                datetime,
                None,
            ]=None,
            **kwargs,
        ):
            """
            Create a BigQuery table on the cloud, returning a DataWarehouse_BigQuery object.

            Parameters:
            - replace           If True, existing table of the same path will be dropped.
            - schema            If proided, new table will be created with this schema.
            - expires           If datetime object provided, new table will expire at this time.
            """

            _table = create_bigquery_table(
                client = get_bigquery_client(),
                table = table,
                replace = replace,
                schema = schema,
                expires = expires,
            )

            if (isinstance(_table, Exception)):
                raise _table
            else:
                return cls(_table)
        


        def rebuild(
            self,
            schema:Iterable[
                Union[
                    Dict[str, Any],
                    bigquery.schema.SchemaField,
                ],
            ]=None,
            expires:Union[
                datetime,
                None,
            ]=None,
            **kwargs,
        ):
            """
            Rebuilds a BigQuery table on the cloud, returning a DataWarehouse_BigQuery object.
            Drops existing table if exists, and replace with blank one.

            Parameters:
            - schema            If provided, new table will be created with this schema.
                                Otherwise, use existing table schema before deletion.
                                If table does not exists and no schema is provided, WarehouseTableNotFound is raised.
            - expires           If datetime object provided, new table will expire at this time.
            """

            # create_bigquery_table will take care of table not existing
            _table = create_bigquery_table(
                client = get_bigquery_client(),
                table = self.bqtable,
                replace = True,
                schema = schema,    # create_bigquery_table will take care of schema being None
                expires = expires,  # create_bigquery_table will take care of expires being None
            )

            if (isinstance(_table, Exception)):
                raise _table
                return _table
            else:
                self.bqtable = _table
                return True

        # TODO
        def query(
            self,
            query:str,
        ):
            pass

        # TODO
        def fetch(
            self,
            fields:Union[
                Iterable[str],
                str,
            ]="*",
            sort:Union[
                Iterable[
                    Tuple[str, QuerySort],
                ],
                None,
            ]=(),
            count:int=10,
            **kwargs,
        ):
            pass

        # TODO
        def load(self):
            pass

        # TODO
        def update(self):
            pass

        def delete(
            self
        ):
            """
            Drop the table
            """

            return drop_bigquery_table(
                client = get_bigquery_client(),
                table = self.bqtable,
                not_found_ok = True,
            )

        drop = delete
    

#=====================================================================================================================================================================
# google.cloud not installed
else:
    class DataWarehouse_BigQuery(DataWarehouseUnavailable):
        # exception will be whichever Exception between google and bigquery
        exception = (google and bigquery)