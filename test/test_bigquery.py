
from collections import namedtuple
from datetime import datetime, timedelta
import unittest
from io import BytesIO
import json
from file_io import file

import pandas as pd
import pytz

from env_context import EnvironmentContext
env_update={
    # Replace with your own json path
    "GOOGLE_APPLICATION_CREDENTIALS":"/opt/credentials/BigQuery/bim-manufacturer-metadata/api-key.json",
}

# Replace with your own project.dataset
TEST_DATASET = "bim-manufacturer-metadata.api_test_dataset"

with EnvironmentContext(update=env_update):
    from test_load_datawarehouse import TestBaseClass, TestLoadDataWarehouse
    from load_datawarehouse.api import google, bigquery
    from load_datawarehouse.bigquery import is_online, \
                                            locations, \
                                            apply_changes_bigquery_table, \
                                            create_bigquery_table, \
                                            get_bigquery_client, \
                                            get_bigquery_table, \
                                            load_bigquery_table, \
                                            set_expiry_bigquery_table, \
                                            drop_bigquery_table
    import load_datawarehouse.bigquery.schema as schema
    from load_datawarehouse.exceptions import WarehouseTableNotFound

from dict_tree import DictionaryTree

if (isinstance(bigquery, Exception)):
    raise bigquery


_client = None 

class TestBigQuery(TestBaseClass): # We can use TestLoadDataWarehouse instead if we want to test everything in the main module too
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

        global _client

        _client = get_bigquery_client()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()

        global _client
        _client.close()

    def setUp(self):
        self.maxDiff = None

        self.test_schema_dicts = [
                                    {
                                        "name": "pkm_familie",
                                        "type": "INTEGER",
                                        "mode": "NULLABLE"
                                    },
                                    {
                                        "name": "t_productfeature",
                                        "type": "RECORD",
                                        "mode": "REPEATED",
                                        "fields": [
                                            {
                                                "name": "Kurztext",
                                                "type": "STRING",
                                                "mode": "NULLABLE"
                                            },
                                            {
                                                "name": "Text",
                                                "type": "STRING",
                                                "mode": "NULLABLE"
                                            },
                                            {
                                                "name": "ID",
                                                "type": "STRING",
                                                "mode": "NULLABLE"
                                            },
                                        ]
                                    },
                                    {
                                        "name": "u_ugr",
                                        "type": "INTEGER",
                                        "mode": "NULLABLE",
                                    },
                                    {
                                        "name": "u_beleuchtungsstaerke_dl_sym_raster",
                                        "type": "INTEGER",
                                        "mode": "NULLABLE",
                                    },
                                    {
                                        "name": "s_einbaudetail",
                                        "type": "STRING",
                                        "mode": "NULLABLE",
                                    },
                                    {
                                        "name": "product_image",
                                        "type": "STRING",
                                        "mode": "NULLABLE",
                                        "fields": [
                                            {
                                                "name": "url",
                                                "type": "STRING",
                                                "mode": "NULLABLE"
                                            },
                                            {
                                                "name": "data",
                                                "type": "STRING",
                                                "mode": "NULLABLE",
                                                "fields": [
                                                    {
                                                        "name": "png",
                                                        "type": "BYTES",
                                                        "mode": "NULLABLE"
                                                    },
                                                    {
                                                        "name": "jpg",
                                                        "type": "BYTES",
                                                        "mode": "NULLABLE"
                                                    },
                                                ]
                                            },
                                        ]
                                    }
                                ]
        self.test_schema_fields = [
                                bigquery.schema.SchemaField(**{
                                    "name": "pkm_familie",
                                    "field_type": "INTEGER",
                                    "mode": "NULLABLE",
                                }),
                                bigquery.schema.SchemaField(**{
                                    "name": "t_productfeature",
                                    "field_type": "RECORD",
                                    "mode": "REPEATED",
                                    "fields": [
                                        bigquery.schema.SchemaField(**{
                                            "name": "Kurztext",
                                            "field_type": "STRING",
                                            "mode": "NULLABLE",
                                        }),
                                        bigquery.schema.SchemaField(**{
                                            "name": "Text",
                                            "field_type": "STRING",
                                            "mode": "NULLABLE",
                                        }),
                                        bigquery.schema.SchemaField(**{
                                            "name": "ID",
                                            "field_type": "STRING",
                                            "mode": "NULLABLE",
                                        }),
                                    ],
                                }),
                                bigquery.schema.SchemaField(**{
                                    "name": "u_ugr",
                                    "field_type": "INTEGER",
                                    "mode": "NULLABLE",
                                }),
                                bigquery.schema.SchemaField(**{
                                    "name": "u_beleuchtungsstaerke_dl_sym_raster",
                                    "field_type": "INTEGER",
                                    "mode": "NULLABLE",
                                }),
                                bigquery.schema.SchemaField(**{
                                    "name": "s_einbaudetail",
                                    "field_type": "STRING",
                                    "mode": "NULLABLE",
                                }),
                                bigquery.schema.SchemaField(**{
                                    "name": "product_image",
                                    "field_type": "STRING",
                                    "mode": "NULLABLE",
                                    "fields": [
                                        bigquery.schema.SchemaField(**{
                                            "name": "url",
                                            "field_type": "STRING",
                                            "mode": "NULLABLE"
                                        }),
                                        bigquery.schema.SchemaField(**{
                                            "name": "data",
                                            "field_type": "STRING",
                                            "mode": "NULLABLE",
                                            "fields": [
                                                bigquery.schema.SchemaField(**{
                                                    "name": "png",
                                                    "field_type": "BYTES",
                                                    "mode": "NULLABLE"
                                                }),
                                                bigquery.schema.SchemaField(**{
                                                    "name": "jpg",
                                                    "field_type": "BYTES",
                                                    "mode": "NULLABLE"
                                                }),
                                            ]
                                        }),
                                    ]
                                })
                            ]
        self.articles_schema_dicts = [
                                        {
                                            "name": "pkm_familie",
                                            "type": "INTEGER",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "b_neuheit",
                                            "type": "BOOLEAN",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_familie",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "u_ugr",
                                            "type": "INTEGER",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "led_module",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_farbe_kombifeld_indirekt",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_abblendraster",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_control",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_baugroesse_list_display_string",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "u_beleuchtungsstaerke_dl_sym_raster",
                                            "type": "INTEGER",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "power_singlefield_with_unit",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_farbe_kombifeld",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "t_productfeature",
                                            "type": "RECORD",
                                            "mode": "REPEATED",
                                            "fields": [
                                                {
                                                    "name": "Kurztext",
                                                    "type": "STRING",
                                                    "mode": "NULLABLE"
                                                },
                                                {
                                                    "name": "Text",
                                                    "type": "STRING",
                                                    "mode": "NULLABLE"
                                                },
                                                {
                                                    "name": "ID",
                                                    "type": "STRING",
                                                    "mode": "NULLABLE"
                                                },
                                            ]
                                        },
                                        {
                                            "name": "s_montageart",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_artikelbild",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_leuchtenlichtstrom_display_unit",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "lumens_with_unit",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "artno_schoen",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "pkm_artikel",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_einbaudetail",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_untertitel",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_gehaeusefarbe",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_leistung_display_unit",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_lichtwerkzeug",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "s_lichtaustritt",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        },
                                        {
                                            "name": "product_image",
                                            "type": "STRING",
                                            "mode": "NULLABLE"
                                        }
                                    ]

    def tearDown(self):
        pass

    def test_connection(self):
        self.assertTrue(is_online(_client))

    def test_convert_schema(self):
        # SIMPLE_CONVERSION_TEST 
        _tests = []

        _test_schema_dicts = self.test_schema_dicts
        _test_schema_fields = self.test_schema_fields

        # Simple Conversion Test from List[dict] to List[SchemaFields]
        _tests.append(
            {
                "args":{
                    "schema":_test_schema_dicts,
                    "dest":"SchemaField",
                },
                "answer":_test_schema_fields,
            }
        )

        # Backward Conversion Test from List[SchemaFields] to List[dict]
        _tests.append(
            {
                "args":{
                    "schema":_test_schema_fields,
                    "dest":"dict",
                },
                "answer":_test_schema_dicts,
            }
        )

        # Assertion Conversion from List[dict] to List[dict]
        _tests.append(
            {
                "args":{
                    "schema":_test_schema_dicts,
                    "dest":"dict",
                },
                "answer":_test_schema_dicts,
            }
        )

        # Assertion Conversion from List[SchemaFields] to List[SchemaFields]
        _tests.append(
            {
                "args":{
                    "schema":_test_schema_fields,
                    "dest":"SchemaField",
                },
                "answer":_test_schema_fields,
            }
        )

        self.conduct_tests(
            schema.convert,
            _tests,
        )

    def test_get_schema_from_dataframe(self):
        _json = self.get_testdata("articles.json", file_like=True)
        _dataframe = pd.read_json(_json)
        
        # _dataframe["t_productfeature"] = _dataframe["t_productfeature"].apply(lambda obj:pd.DataFrame.from_records(obj) if obj else None)
        # DictionaryTree(get_schema_from_dataframe(_dataframe))
        self.assertCountEqual(
            schema.extract(
                _dataframe,
                self.test_schema_fields, # We have to provide a default schema, otherwise pandas will assume all empty fields as FLOAT, which is different from get_api_repr_from_record_fields(), which defaults to STRING.
            ),
            self.articles_schema_dicts,
        )

    def test_get_schema_from_json(self):
        _json = self.get_testdata("articles.json", file_like=True)
        _dict = json.load(_json)

        self.assertCountEqual(
            schema.extract(
                _dict
            ),
            self.articles_schema_dicts,
        )
    
    def test_set_expiry_bigquery_table(self):
        global _client

        _test_table = f"{TEST_DATASET}.api_test_table"
        _7_days_from_now = (datetime.utcnow() + timedelta(days = 7)).replace(microsecond=0, tzinfo=pytz.utc)
        _never = None
        
        def _func(
            client,
            table,
            expires,
        ):
            table = set_expiry_bigquery_table(client, table, expires)
            return table.expires

        _tests = [
            {
                "args":{
                    "client": _client,
                    "table": _test_table,
                    "expires": _7_days_from_now,
                },
                "answer": _7_days_from_now,
            },
            {
                "args":{
                    "client": _client,
                    "table": _test_table,
                    "expires": _never,
                },
                "answer": _never,
            },
        ]

        self.conduct_tests(
            _func,
            _tests
        )

    def test_create_set_drop_table(self):
        global _client

        _test_table = f"{TEST_DATASET}.api_create_drop_table"
        _1_hour_from_now = (datetime.utcnow() + timedelta(hours = 1)).replace(microsecond=0, tzinfo=pytz.utc)
        # _location = locations.EUROPE_WEST2

        table = create_bigquery_table(
            _client,
            _test_table,
            replace=True,
            schema=self.articles_schema_dicts,
            expires=_1_hour_from_now,
        )

        self.assertIsInstance(table, bigquery.table.Table)

        # load_bigquery_table(
        #     _client,
        #     table,
            
        # )

        drop_bigquery_table(
            _client,
            table,
            not_found_ok=True,
        )

        self.assertIsInstance(
            get_bigquery_table(_client, _test_table),
            WarehouseTableNotFound
        )

        

if __name__ == "__main__":
    with EnvironmentContext(
        update=env_update
    ):
        unittest.main()