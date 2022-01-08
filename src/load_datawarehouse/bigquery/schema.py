import enum
from collections import OrderedDict
from typing import  Any, \
                    Dict, \
                    Iterable, \
                    List, \
                    Union
import warnings
from load_datawarehouse.exceptions import WarehouseInvalidInput

import numpy as np
import pandas as pd

try:
    from dict_tree import DictionaryTree
except (ImportError, ModuleNotFoundError):
    class DictionaryTree():
        def __init__(
            self,
            *args,
            **kwargs,
        ):
            warnings.warn("Optional module DictionaryTree not installed.")

from load_datawarehouse.api import google, bigquery
from load_datawarehouse.config import   MAX_FACTOR_OF_RECORDS_WHICH_ADDS_FIELDS, \
                                        MIN_RECORDS_TO_TRIGGER_DIFF_CHECK
import load_datawarehouse.schema
from load_datawarehouse.schema import ListField, DeconstructedRecords, DeconstructedList, is_records



if (hasattr(bigquery._pandas_helpers, "_PANDAS_DTYPE_TO_BQ")):
    _PANDAS_DTYPE_TO_BQ = bigquery._pandas_helpers._PANDAS_DTYPE_TO_BQ
else:
    # If the code has changed, use the cached version
    # https://github.com/googleapis/python-bigquery/blob/750c15d8c7449776955237d01c24be7b4b66d0b5/google/cloud/bigquery/_pandas_helpers.py#L79
    _PANDAS_DTYPE_TO_BQ = {
        "bool": "BOOLEAN",
        "datetime64[ns, UTC]": "TIMESTAMP",
        "datetime64[ns]": "TIMESTAMP",
        "float32": "FLOAT",
        "float64": "FLOAT",
        "int8": "INTEGER",
        "int16": "INTEGER",
        "int32": "INTEGER",
        "int64": "INTEGER",
        "uint8": "INTEGER",
        "uint16": "INTEGER",
        "uint32": "INTEGER",
        "geometry": "GEOGRAPHY",
    }


class SchemaFieldProperties(enum.Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

class SchemaFieldType(SchemaFieldProperties):
    STRING      = enum.auto()
    BYTES       = enum.auto()
    INTEGER     = enum.auto()
    INT64       = INTEGER
    FLOAT       = enum.auto()
    FLOAT64     = FLOAT
    BOOLEAN     = enum.auto()
    BOOL        = BOOLEAN
    TIMESTAMP   = enum.auto()
    DATE        = enum.auto()
    TIME        = enum.auto()
    DATETIME    = enum.auto()
    GEOGRAPHY   = enum.auto()
    NUMERIC     = enum.auto()
    BIGNUMERIC  = enum.auto()
    RECORD      = enum.auto()

class SchemaFieldMode(SchemaFieldProperties):
    NULLABLE    = enum.auto()
    REQUIRED    = enum.auto()
    REPEATED    = enum.auto()


def guess_bq_dtype(
    types:Iterable[
        Union[
            np.dtype,
            type,
            tuple,
        ]
    ],
    force_numeric:bool=False,
):
    return load_datawarehouse.schema.guess_warehouse_dtype(
        types=types,
        warehouse_dtype_mapper=_PANDAS_DTYPE_TO_BQ,
        force_numeric=force_numeric,
    )




def build_api_repr(
    name:str,
    type_:SchemaFieldType=SchemaFieldType.STRING,
    mode:SchemaFieldMode=SchemaFieldMode.NULLABLE,
    fields:Iterable[
        Union[
            Dict[str,Any],
            bigquery.schema.SchemaField,
        ],
    ]=None,
    description:str="",
    policyTags:Dict[str, List[str]]={},
    maxLength:int=None,
    precision:int=None,
    scale:int=None,
):
    """
    A simple function to build an api_repr dict.
    This is mostly implemented to allow for sanitation and checking later if rules need to be strict.
    """

    if (name and type_ and mode):
        _api_repr = {
            "name": name,
            "type": type_.value,
            "mode": mode.value,
        }

        if (fields):        _api_repr["fields"] = fields
        if (description):   _api_repr["description"] = description
        if (policyTags):    _api_repr["policyTags"] = policyTags
        
        if (type_ in (
            SchemaFieldType.STRING,
            SchemaFieldType.BYTES,
        )):
            if (maxLength): _api_repr["maxLength"] = maxLength

        if (type_ in (
            SchemaFieldType.NUMERIC,
            SchemaFieldType.BIGNUMERIC,
        )):
            if (precision): _api_repr["precision"] = precision

        if (scale):         _api_repr["scale"] = scale

        return _api_repr
    else:
        raise WarehouseInvalidInput(f"Error building api_repr for SchemaField - name ({name}), type ({type}) and mode ({mode}) need to be set.")




def get_api_repr_from_default(
    name:str,
    default:Union[
        str,
        Dict[str,Any],
        SchemaFieldType,
        tuple,
    ]="STRING",
):
    if (isinstance(default, str)):
        default = SchemaFieldType(default)

    if (isinstance(default, SchemaFieldType)):
        _api_repr = {
            "type_":default,
            "mode":SchemaFieldMode.NULLABLE,
        }
    elif (is_records(default)):
        _api_repr = {
            "type_":SchemaFieldType.RECORD,
            "mode":SchemaFieldMode.REPEATED,
            "fields":get_api_repr_from_record_fields(default),
        }
    elif (isinstance(default, Dict)):
        _api_repr = default
    else:
        _api_repr = {
            "type_":SchemaFieldType.STRING,
            "mode":SchemaFieldMode.NULLABLE,
        }

    return build_api_repr(
        name=name,
        **_api_repr,
    )
        
        
        




def is_api_repr(
        obj:Union[any, Dict[str,str]]
    ):
        return ("name" in obj and \
                "mode" in obj and \
                "type" in obj) if (isinstance(obj, dict)) else False





def convert_schema_to_api_repr(
    schema:Iterable[
        bigquery.schema.SchemaField,
    ]
):
    def _conversion(obj:any):
        if (isinstance(obj, bigquery.schema.SchemaField)):
            _api_repr = obj.to_api_repr()
            if (obj.fields):
                _api_repr["fields"] = convert_schema_to_api_repr(obj.fields)
            return _api_repr
        elif (is_api_repr(obj)):
                # This is already an api_repr - not sure why but we'll leave it.
                return obj
        else:
            # This is not a valid input. Consider issuing warning?
            return obj

    return [
        _conversion(obj) for obj in schema
    ]





def convert_api_repr_to_schema(
    schema:Iterable[
        dict,
    ]
):
    def _conversion(obj:any):
        if (is_api_repr(obj)):
            return bigquery.schema.SchemaField.from_api_repr(obj)
        elif (isinstance(obj, bigquery.schema.SchemaField)):
            return obj
        else:
            # This is not a valid input. Consider issuing warning?
            return obj
                
    return [
        _conversion(obj) for obj in schema
    ]





def convert(
    schema:Iterable[
        Union[
            Dict[str,str],
            bigquery.schema.SchemaField,
        ]
    ],
    dest:str="dict",
)->Iterable[Dict[str,str]]:
    
    _type_switch = {
        "dict": convert_schema_to_api_repr,
        "dicts": convert_schema_to_api_repr,
        "record": convert_schema_to_api_repr,
        "records": convert_schema_to_api_repr,
        "SchemaField": convert_api_repr_to_schema,
        "SchemaFields": convert_api_repr_to_schema,
    }

    _type_switch[dict] = _type_switch.get("dict")
    _type_switch[bigquery.schema.SchemaField] = _type_switch.get("SchemaField")

    if (dest not in _type_switch):
        raise WarehouseInvalidInput(f"dest '{dest}' is not a valid input for convert_schema().")
    else:
        return _type_switch[dest](schema)



# TODO - Rewrite this with DictionaryTree(); this predates that class, which was actually written with scripts copied from here.

def describe(
    schema:Union[
        bigquery.table.Table,
        Iterable[
            Union[
                bigquery.schema.SchemaField,
                Dict
                ]
            ]
        ],
    template:"OrderedDict[str, Union[int, None]]"={
        "name":60,
        "type":30,
        "mode":10,
    },
    layers:tuple=(),
    indent:int=4,
    echo:bool=True,
)->str:
    '''
    Print to stdout a description of the table schema, for example:

    Table [Erco_articles]
    ├── pkm_familie                                                 INTEGER                       NULLABLE  
    ├── b_neuheit                                                   BOOLEAN                       NULLABLE  
    ├── s_familie                                                   STRING                        NULLABLE  
    ├── u_ugr                                                       FLOAT                         NULLABLE  
    ├── led_module                                                  STRING                        NULLABLE  
    ├── s_farbe_kombifeld_indirekt                                  STRING                        NULLABLE  
    ├── s_abblendraster                                             STRING                        NULLABLE  
    ├── s_control                                                   STRING                        NULLABLE  
    ├── s_baugroesse_list_display_string                            STRING                        NULLABLE  
    ├── u_beleuchtungsstaerke_dl_sym_raster                         FLOAT                         NULLABLE  
    ├── power_singlefield_with_unit                                 STRING                        NULLABLE  
    ├── s_farbe_kombifeld                                           STRING                        NULLABLE  
    ├─┬ t_productfeature                                            RECORD                        REPEATED  
    │ ├── ID                                                        STRING                        NULLABLE  
    │ ├── Kurztext                                                  STRING                        NULLABLE  
    │ └── Text                                                      STRING                        NULLABLE  
    ├── s_montageart                                                STRING                        NULLABLE  
    ├── s_artikelbild                                               STRING                        NULLABLE  
    ├── s_leuchtenlichtstrom_display_unit                           STRING                        NULLABLE  
    ├── lumens_with_unit                                            STRING                        NULLABLE  
    ├── artno_schoen                                                STRING                        NULLABLE  
    ├── pkm_artikel                                                 STRING                        NULLABLE  
    ├── s_einbaudetail                                              FLOAT                         NULLABLE  
    ├── s_untertitel                                                STRING                        NULLABLE  
    ├── s_gehaeusefarbe                                             STRING                        NULLABLE  
    ├── s_leistung_display_unit                                     STRING                        NULLABLE  
    ├── s_lichtwerkzeug                                             STRING                        NULLABLE  
    ├── s_lichtaustritt                                             STRING                        NULLABLE  
    └── product_image                                               STRING                        NULLABLE  
    '''
    # Chars to draw the tree
    BOX_SPACE = u" "
    BOX_HORIZONTAL = u"\u2500"
    BOX_VERTICAL = u"\u2502"
    BOX_ANGLE_TOP_RIGHT = u"\u2514"
    BOX_VBRANCH_TO_RIGHT = u"\u251C"
    BOX_HBRANCH_TO_BOTTOM = u"\u252C"
    LINE_BREAK = u"\n"

    # if its a table, get the schema first
    if (isinstance(schema, (
        bigquery.table.Table,
        # bigquery.table.TableReference,
        # bigquery.table.TableListItem,
    ))):
        # f"" already implies fu"" (python>=3.6); the latter is unsupported.
        # ...how disappointing.
        _title = f"Table [{schema.table_id}]"+ LINE_BREAK
        schema = schema.schema
    else:
        if (len(layers) <= 0):
            _title = u"Table schema"+ LINE_BREAK
        else:
            _title = u""
    
    # Generate template
    template_string = "".join(
        [
            f"{{{_formatter}{(':'+str(template[_formatter])+'s') if (template[_formatter]) else ''}}}" \
                for _formatter in template
        ]
    )
    
    _return = _title

    _tree_indent = "".join([
        (BOX_VERTICAL if (_layer) else BOX_SPACE) + max(0, indent-1)*BOX_SPACE \
            for _layer in layers
    ])

    for _id, _field in enumerate(schema):
        _is_last_field = (_id>=(len(schema)-1))
        _branch_char = BOX_ANGLE_TOP_RIGHT if (_is_last_field) else BOX_VBRANCH_TO_RIGHT

        _tree_field = _tree_indent + _branch_char + max(1, indent)*BOX_HORIZONTAL + BOX_SPACE
        _tree_record = _tree_indent + _branch_char + max(0, indent-1)*BOX_HORIZONTAL + BOX_HBRANCH_TO_BOTTOM + BOX_SPACE

        # Make a subtemplate and reduce the first indentation by indent
        _subtemplate = template.copy()
        _subtemplate[next(iter(_subtemplate.keys()))] -= indent

        if (isinstance(_field, bigquery.schema.SchemaField)):
            # Dicts are easier to work with here.
            _field = _field.to_api_repr()

        if (isinstance(_field, dict)):
            _field_description = template_string.format(**_field)
            
            if (_field.get("fields", ())):
                # If it is a record having subfields, add a branch and call itself.
                _return += _tree_record + _field_description + LINE_BREAK
                _return += describe(
                    schema=_field["fields"],
                    template=_subtemplate,
                    layers=(*layers, not _is_last_field),
                    indent=indent,
                    echo=False,
                )
            else:
                # If it is a just a field, no branch.
                _return += _tree_field + _field_description + LINE_BREAK
        else:
            # Unknown type passed to parser.
            _return += _tree_field+u"[UNKNOWN FIELD {}]".format(str(_field))+LINE_BREAK

    # This is not for debug - this is a describe function, we have to get it to stdout!
    if (echo):
        print (_return)

    # Returning it for good measure; not necessary.
    return _return




def get_api_repr_from_record_fields(
    record_field: tuple,
    schema:Iterable[
        Union[
            Dict[str,str],
            bigquery.schema.SchemaField,
        ]
    ]=None,
    default:Union[
        str,
        Dict[str,Any],
        SchemaFieldType,
        tuple,
    ]="STRING",
)->Iterable[Dict[str,str]]:
    """
    Takes a CONDENSED RecordFields object and produce the BigQuery Schema in api_repr form (i.e. dict).

    """

    if (is_records(record_field)):
        _bq_schema = []

        for _field, _type in zip(record_field._fields, record_field):

            if (_type is None):
                # Default Field
                if (default is not None):
                    _bq_schema.append(
                        get_api_repr_from_default(
                            name = _field,
                            default = default,
                        )
                    )
                else:
                    # if default is None, skip the field.
                    continue
            elif (isinstance(_type, str)):
                # NULLABLE
                _bq_schema.append(
                    build_api_repr(
                        name = _field,
                        type_= SchemaFieldType(_type),
                        mode = SchemaFieldMode.NULLABLE,
                        fields = None,
                    )
                )
            elif (is_records(_type)):
                # RECORD
                _bq_schema.append(
                    build_api_repr(
                        name = _field,
                        type_= SchemaFieldType.RECORD,
                        mode = SchemaFieldMode.REPEATED,
                        fields = get_api_repr_from_record_fields(
                            _type
                        )
                    )
                )
            elif (isinstance(_type, ListField)):
                # REPEATED
                _bq_schema.append(
                    build_api_repr(
                        name = _field,
                        type_= SchemaFieldType(_type[0]),
                        mode = SchemaFieldMode.REPEATED,
                        fields = None,
                    )
                )

        return _bq_schema
    else:
        raise WarehouseInvalidInput(f"get_api_repr_from_record_fields expects RecordFields as input; {type(record_field).__name__} found.")



def get_schema_from_records(
    obj:Iterable[
        Dict[str, Any]
    ],
    schema:Union[
        Iterable[
            bigquery.schema.SchemaField,
        ],
        Dict,
    ] = {},
)->Iterable[Dict[str,Any]]:
    _deconstructed = load_datawarehouse.schema.deconstruct_records(
        obj
    )

    if (isinstance(_deconstructed, load_datawarehouse.schema.DeconstructedRecords)):
        _condensed_schema = load_datawarehouse.schema.condense_record_fields(
            _deconstructed.fields,
            warehouse_dtype_mapper=_PANDAS_DTYPE_TO_BQ,
            force_numeric=False,
            schema=schema,
        )

        return get_api_repr_from_record_fields(_condensed_schema, schema, )
    elif (isinstance(_deconstructed, load_datawarehouse.schema.DeconstructedList)):
        # Its a simple list
        pass



class SchemaFromDataframeMethod(SchemaFieldProperties):
    SEARCH_VALUES = enum.auto()
    GOOGLE_NATIVE = enum.auto()

def get_schema_from_dataframe(
    dataframe:pd.DataFrame,
    schema:Union[
        Iterable[
            bigquery.schema.SchemaField,
        ],
        Dict,
    ] = {},
    method:SchemaFromDataframeMethod = SchemaFromDataframeMethod.SEARCH_VALUES,
)->Iterable[Dict[str,Any]]:
    _method_switch = {
        SchemaFromDataframeMethod.SEARCH_VALUES:get_schema_from_dataframe_search_values,
        SchemaFromDataframeMethod.GOOGLE_NATIVE:get_schema_from_dataframe_google_native,
    }
    
    return _method_switch.get(
        method,
        _method_switch.get(
            SchemaFromDataframeMethod.SEARCH_VALUES # Default method if not found
        )
    )(
        # Call the function
        dataframe=dataframe,
        schema=schema,
    )

def get_schema_from_dataframe_search_values(
    dataframe:pd.DataFrame,
    schema:Union[
        Iterable[
            bigquery.schema.SchemaField,
        ],
        Dict,
    ] = {},
)->Iterable[Dict[str,Any]]:
    if (isinstance(dataframe, pd.DataFrame)):
        _records = dataframe.to_dict(
            orient="records",
        )
        
        return get_schema_from_records(
                _records,
                schema=schema,
            )
    else:
        raise WarehouseInvalidInput(
            f"Pandas Dataframe expected; {type(dataframe).__name__} found."
        )

def get_schema_from_dataframe_google_native(
    dataframe:pd.DataFrame,
    schema:Union[
        Iterable[
            bigquery.schema.SchemaField,
        ],
        Dict,
    ] = {},
)->Iterable[Dict[str,Any]]:
    # Internal module of google.cloud.bigquery
    # https://github.com/googleapis/python-bigquery/blob/main/google/cloud/bigquery/_pandas_helpers.py

    return bigquery._pandas_helpers.dataframe_to_bq_schema(
        dataframe,
        schema,
    )

def extract(
    obj:Union[
        Iterable[Dict[str,Any]],
        pd.DataFrame,
    ],
    schema:Union[
        Iterable[
            bigquery.schema.SchemaField,
        ],
        Dict,
    ] = {},
):
    if (isinstance(obj, list)):
        return get_schema_from_records(
            obj,
            schema=schema,
        )
    elif (isinstance(obj, pd.DataFrame)):
        return get_schema_from_dataframe(
            dataframe=obj,
            schema=schema,
            method=SchemaFromDataframeMethod.SEARCH_VALUES,
        )
    else:
        raise WarehouseInvalidInput(f"List of Dicts or Pandas DataFrame expected, {type(obj).__name__} found.")

if __name__=="__main__":


    _records = [
        {
            "A":1,
            "B":2,
            "C":3,
        },
        {
            "A":1.23,
            "B":True,
            "C":56
        },
        {
            "A":56,
            "B":"Google",
            "D":[
                {
                    "D1": True,
                    "D2": False,
                    "D3": [
                        {
                            "D3a":123
                        }
                    ]
                },
                {
                    "D1": True,
                    "D2": False,
                    "D3": [
                        {
                            "D3a":456,
                            "D3b":"Something",
                        }
                    ]
                }
            ],
        },
        None,
        123,
        {
            "E":None,
            "FFF":666
        },
        {
            "G":123
        },
        {
            "G":[
                1,2,3,4,5,6,7,8,9,10
            ]
        },
        {
            "FFF": 456.123,
            "G":[
                2,3,4,5,6,1
            ]
        },
    ]

    import pandas as pd
    _deconstructed = load_datawarehouse.schema.deconstruct_records(_records)

    DictionaryTree(_deconstructed, indent=3)

    print ("\n"*3)

    DictionaryTree(load_datawarehouse.schema.condense_record_fields(_deconstructed.fields))