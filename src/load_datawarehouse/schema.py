from collections import namedtuple
from copy import copy
from datetime import date, datetime, time
import re
from typing import List, OrderedDict, Union, Iterable, Dict, Any

import numpy as np
import pandas as pd

from load_datawarehouse.config import   MIN_RECORDS_TO_TRIGGER_DIFF_CHECK, \
                                        MAX_FACTOR_OF_RECORDS_WHICH_ADDS_FIELDS                                

from load_datawarehouse.api import bigquery

import load_datawarehouse.data

from ordered_set import OrderedSet

# WARNING: This module is a bit of a botch.
# -----
# The main purpose of this is to create schemas from existing set of data automatically by looking through each and every record of data.
# It is very intensive in heavy data sets; but you are supposed to run this once at the first creation of your table in the warehouse.
# When the table alerady exists, you can just get the existing schema and feed it into the API, which will make assumptions based on the existing schema, speeding things up by quite a bit.
# 
# There is some tolerance in the data. Examples that it will not put up with:
# - a mix of sub-Records, Lists and/or Scalar values between Records using the same key;
# - a mix of strings and numbers between Records usin the same key; they will ALL be treated as STRINGS.
#
# There is simply no good fixed rules to "guess" a schema out of a list of dicts; if a schema is known, we should write it manually.
# Use this for quick testing only.

DeconstructedRecords = namedtuple("DeconstructedRecords", [
    "fields",
    "factor_of_records_adding_fields",
    "records",
    "type_errors",
])

DeconstructedList = namedtuple("DeconstructedList", [
    "types",
    "list",
    "type_errors",
])

field_name_switch = {
    dict: lambda _dict: _dict.get("name", None),
    bigquery.schema.SchemaField: lambda _bq_schema_field: _bq_schema_field.name,
}

field_type_switch = {
    dict: lambda _dict: _dict.get("type", None),
    bigquery.schema.SchemaField: lambda _bq_schema_field: _bq_schema_field.field_type,
}

sub_fields_switch = {
    dict: lambda _dict: _dict.get("fields", []),
    bigquery.schema.SchemaField: lambda _bq_schema_field: _bq_schema_field.fields,
}



# This class does not need to do much; its simply for the purpose of isinstance(obj, ListField)
class ListField(tuple):
    pass

# This determines if something is a generic schema - this is not for checking if things are actually a List[Dict[str, Any]]!
def is_records(
    obj:any
    ):
    if (isinstance(obj, type)):
        _type = obj
        return (issubclass(_type, tuple) and \
                _type.__name__ == "RecordFields")
    else:
        _instance = obj
        return (isinstance(_instance, tuple) and \
                type(_instance).__name__ == "RecordFields")

def expand_iterable(
    obj:Iterable,
):
    if (isinstance(
        obj,
        (
            # Things that iterate but are not iterables
            str,
        )        
    )):
        yield obj
    else:
        if (isinstance(
            obj,
            (
                dict,
            )
        )):
            _iter = obj.values()
        else:
            _iter = copy(obj)

        try:
            for _item in _iter:
                for _expanded in expand_iterable(_item):
                    yield _expanded
        except TypeError as e:
            yield obj

def _contains_recordfields(
        types:Iterable,
    ):
    for _type in types:
        if (is_records(_type)):
            return True
    
    return False

def _contains_listfield(
        types:Iterable,
    ):
    for _type in types:
        if (isinstance(_type, ListField)):
            return True
    
    return False

def get_field_from_schema(
    field_name:str,
    schema:Union[
        Iterable[
            bigquery.schema.SchemaField,
        ],
        Dict,
    ],
    convert_to_api_repr:bool=True,
):
    for _field in schema:        
        _field_name = field_name_switch.get(
            type(_field),
            None
        )(_field)

        if (_field_name == field_name):
            if (convert_to_api_repr and isinstance(_field, bigquery.schema.SchemaField)):
                # TODO: This does not currently deal with sub-fields at all
                _field = _field.to_api_repr()

            return _field

    return None

def convert_schema_field_to_record_field(
    schema:Union[
        Iterable[
            Union[
                bigquery.schema.SchemaField,
                Dict[str,Any],
            ],
        ],
        Union[
            bigquery.schema.SchemaField,
            Dict,
        ],
    ],
)->tuple:
    _fields = {}

    for _field in schema:
        # Get the Field name and Sub Fields,
        #   using the switch dicts declared at the start of this module.
        _field_name, _field_type, _sub_fields = (
            switch.get(
                type(_field)
            )(_field) \
                for switch in (
                    field_name_switch,
                    field_type_switch,
                    sub_fields_switch
                )
        )
        
        # This function self-protects against fields having invalid names - but these should have been done at data level before calling this function.
        _cleaned_field_name = load_datawarehouse.data.clean_field_key(_field_name)

        if (_sub_fields):
            # its a RECORD
            _fields[_cleaned_field_name] = convert_schema_field_to_record_field(
                _sub_fields
            )
        else:
            # its SCALAR
            _fields[_cleaned_field_name] = _field_type
    
    return namedtuple("RecordFields",
                    field_names=_fields.keys())(
                    **_fields,
                )

def condense_record_fields(
    record_fields:tuple,
    warehouse_dtype_mapper:Dict[str,str],
    force_numeric:bool=False,
    schema:Union[
        Iterable[
            bigquery.schema.SchemaField,
        ],
        Dict,
    ] = [],
):
    # This internal function expects the output from deconstruct_records(), which produces tuple[RecordFields].
    # Direct calling on a single RecordFields is not actually supported, but we will take care of that anyway.
    if (is_records(record_fields)):
        record_fields = (record_fields, )

    # At this point we are expecting the structure to be:
    # (
    #   RecordFields(
    #       key1:(
    #           type1,
    #           type2,
    #           ...
    #       ),
    #       key2:(
    #           type3,
    #           type4,
    #           ...
    #       ),
    #       key3:(
    #           RecordFields(
    #               key31:(
    #                   type1,
    #                   type2,
    #               ),
    #               key32:(
    #                   type3,
    #                   type4,
    #               ),
    #               ...
    #           ),
    #           RecordFields(
    #               ...
    #           ),
    #           RecordFields(
    #               ...
    #           ),
    #           ...
    #       )
    #   ),
    #   RecordFields(
    #       ...
    #   ),
    #   RecordFields(
    #       ...
    #   ),
    #   ...
    # )

    _type_mappings = {}

    for _record_field in record_fields:
        if (is_records(_record_field)):
            for _field, _types in zip(_record_field._fields, _record_field):
                _type_mappings[_field] = _type_mappings.get(_field, OrderedSet()) | OrderedSet(_types)

    # Now we have a _type_mappings like this:
    # {
    #     key1:(type1, type2,...),
    #     key2:(type3, type4,...),
    #     key3:(RecordFields(...), RecordFields(...), RecordFields(...),...)
    # }

    _record_fields_condensed = {}
    
    if (not isinstance(schema, list)):
        schema = []

    for _field_name, _field_types in zip(_type_mappings, _type_mappings.values()):
        # This function self-protects against fields having invalid names - but these should have been done at data level before calling this function.
        _cleaned_field_name = load_datawarehouse.data.clean_field_key(_field_name)

        if (_existing_field := get_field_from_schema(_field_name, schema, convert_to_api_repr=True)):
            # If the existing schema has a record for it, then juse use that
            _existing_field_name, _existing_field_type, _existing_sub_fields = (
                switch.get(
                    type(_existing_field)
                )(_existing_field) \
                    for switch in (
                        field_name_switch,
                        field_type_switch,
                        sub_fields_switch
                    )
            )

            if (_existing_sub_fields):
                _record_fields_condensed[_cleaned_field_name] = convert_schema_field_to_record_field(_existing_sub_fields)
            else:
                _record_fields_condensed[_cleaned_field_name] = _existing_field_type
        elif (_contains_recordfields(_field_types)):
            # If any type is a RecordFields, we condense it down to one
            _record_fields_condensed[_cleaned_field_name] = condense_record_fields(_field_types, warehouse_dtype_mapper=warehouse_dtype_mapper, force_numeric=force_numeric, schema=_existing_field)
        elif (_contains_listfield(_field_types)):
            _record_fields_condensed[_cleaned_field_name] = condense_list_fields(_field_types, warehouse_dtype_mapper=warehouse_dtype_mapper, force_numeric=force_numeric)
        else:
            # Otherwise  
            _record_fields_condensed[_cleaned_field_name] = guess_warehouse_dtype(_field_types, warehouse_dtype_mapper=warehouse_dtype_mapper, force_numeric=force_numeric)

    # This condenses all the fields down to the best suited dtype:
    # {
    #         key1:bq_type1,
    #         key2:bq_type2,
    #         key3:RecordFields(
    #             key31:bq_type31,
    #             key32:bq_type32,
    #             ...
    #         ),
    #         ...
    #     )
    # }

    return namedtuple("RecordFields",
                    field_names=_record_fields_condensed.keys())(
                    **_record_fields_condensed,
                )

    

def condense_list_fields(
    list_field:Iterable[ListField],
    warehouse_dtype_mapper:Dict[str,str],
    force_numeric:bool=False,
):
    # This internal function expects the output from deconstruct_records(), which produces tuple[ListField].
    # Direct calling on a single ListField is not actually supported, but we will take care of that anyway.
    if (isinstance(list_field, ListField)):
        list_field = (list_field, )

    _all_types = OrderedSet(
        expand_iterable(list_field)
    )

    return ListField(
        (
            guess_warehouse_dtype(_all_types, warehouse_dtype_mapper=warehouse_dtype_mapper, force_numeric=force_numeric),
        )
    )

def guess_warehouse_dtype(
    types:Iterable[
        Union[
            np.dtype,
            type,
            tuple,
        ]
    ],
    warehouse_dtype_mapper:Dict[str,str],
    force_numeric:bool=False,
):
    '''
    Guess the best bigquery data type from an iterable of types.
    Note that the input is of TYPES, not INSTANCES of the types.
    '''

    types = tuple(types) # Make it a tuple to get around StopIteration on generators

    if (not force_numeric):
        _type_switch = OrderedDict({
            bytes: "BYTES",
            datetime: "DATETIME",
            date: "DATE",
            time: "TIME",
            str: "STRING",
        })
        
        for _type in types:
            for _py_dtype, _warehouse_dtype in zip(_type_switch.keys(), _type_switch.values()):
                if (issubclass(_py_dtype, _type)):
                    return _warehouse_dtype

    _np_dtype = np.find_common_type(
        [],
        list(types)
    )

    if (_np_dtype and \
        _np_dtype is not np.dtype("O")):
        _warehouse_dtype = warehouse_dtype_mapper.get(
            _np_dtype.name,
            None
        )
    else:
        _warehouse_dtype = None

    return _warehouse_dtype

    

def deconstruct_records(
    records:Iterable[
        dict # in programming terms, this function will allow records to be a generator object. However it will still consume the generator making it a bit useless.
    ],
):
    # Internal function.
    # Takes a List[Dict[]], and returns a namedtuple of type DeconstructedRecords with the following fields:
    #   "fields"
    #       A namedtuple of type RecordFields, containing a named field for each key found inside at least one of the records.
    #       Each field will contain a tuple of type objects - representing all the unique types of object found under that key.
    #       If the field is nested, it will contain a tuple of RecordFields instead.
    #
    #       
    #   "factor_of_records_adding_fields"
    #       A float indicating how many RecordFields added additional fields during iteration. It is a very rudimentary method of estimating whether records are actually records, or a collection of unstructured dicts.
    #       
    #   "records"
    #       This is a list of all the dicts found. Anything that is not a dict (i.e. a record) will be excluded.
    #       
    #   "type_errors"
    #       This is a list of all the non-dicts found. Evaluate these to warn the users of dropped data.
    # 
    #   Example Input:
    # _records = [
    #         {
    #             "A":1,
    #             "B":2,
    #             "C":3,
    #         },
    #         {
    #             "A":1.23,
    #             "B":True,
    #             "C":56
    #         },
    #         {
    #             "A":56,
    #             "B":"Google",
    #             "D":[
    #                 {
    #                     "D1": True,
    #                     "D2": False,
    #                     "D3": [
    #                         {
    #                             "D3a":123
    #                         }
    #                     ]
    #                 },
    #                 {
    #                     "D1": True,
    #                     "D2": False,
    #                     "D3": [
    #                         {
    #                             "D3a":456,
    #                             "D3b":"Something",
    #                         }
    #                     ]
    #                 }
    #             ],
    #         },
    #         None,
    #         123,
    #         {
    #             "E":None,
    #             "FFF":666
    #         },
    #         {
    #             "G":123
    #         },
    #         {
    #             "G":[
    #                 1,2,3,4,5,6,7,8,9,10
    #             ]
    #         },
    #         {
    #             "FFF": 456.123,
    #             "G":[
    #                 2,3,4,5,6,1
    #             ]
    #         },
    #     ]
        
    #   Example Output:
    # ╾──┬ Instance of [DeconstructedRecords] at address [0x7f1d733e5720]    DeconstructedRecords    
    #    ├──┬ fields                                                         RecordFields            
    #    │  ├──┬ B                                                           tuple                   
    #    │  │  ├─── [0]                                                      type                    <class 'bool'>
    #    │  │  ├─── [1]                                                      type                    <class 'str'>
    #    │  │  └─── [2]                                                      type                    <class 'int'>
    #    │  ├──┬ C                                                           tuple                   
    #    │  │  └─── [0]                                                      type                    <class 'int'>
    #    │  ├──┬ A                                                           tuple                   
    #    │  │  ├─── [0]                                                      type                    <class 'float'>
    #    │  │  └─── [1]                                                      type                    <class 'int'>
    #    │  ├──┬ D                                                           tuple                   
    #    │  │  └──┬ [0]                                                      RecordFields            
    #    │  │     ├──┬ D1                                                    tuple                   
    #    │  │     │  └─── [0]                                                type                    <class 'bool'>
    #    │  │     ├──┬ D2                                                    tuple                   
    #    │  │     │  └─── [0]                                                type                    <class 'bool'>
    #    │  │     └──┬ D3                                                    tuple                   
    #    │  │        ├──┬ [0]                                                RecordFields            
    #    │  │        │  └──┬ D3a                                             tuple                   
    #    │  │        │     └─── [0]                                          type                    <class 'int'>
    #    │  │        └──┬ [1]                                                RecordFields            
    #    │  │           ├──┬ D3a                                             tuple                   
    #    │  │           │  └─── [0]                                          type                    <class 'int'>
    #    │  │           └──┬ D3b                                             tuple                   
    #    │  │              └─── [0]                                          type                    <class 'str'>
    #    │  ├─── E                                                           tuple                   
    #    │  ├──┬ FFF                                                         tuple                   
    #    │  │  ├─── [0]                                                      type                    <class 'float'>
    #    │  │  └─── [1]                                                      type                    <class 'int'>
    #    │  └──┬ G                                                           tuple                   
    #    │     ├──┬ [0]                                                      ListField               
    #    │     │  └─── [0]                                                   type                    <class 'int'>
    #    │     └─── [1]                                                      type                    <class 'int'>
    #    ├─── factor_of_records_adding_fields                                float                   0.3333333333333333
    #    ├──┬ records                                                        list                    
    #    │  ├──┬ [0]                                                         dict                    
    #    │  │  ├─── A                                                        int                     1
    #    │  │  ├─── B                                                        int                     2
    #    │  │  └─── C                                                        int                     3
    #    │  ├──┬ [1]                                                         dict                    
    #    │  │  ├─── A                                                        float                   1.23
    #    │  │  ├─── B                                                        bool                    True
    #    │  │  └─── C                                                        int                     56
    #    │  ├──┬ [2]                                                         dict                    
    #    │  │  ├─── A                                                        int                     56
    #    │  │  ├─── B                                                        str                     Google
    #    │  │  └──┬ D                                                        list                    
    #    │  │     ├──┬ [0]                                                   dict                    
    #    │  │     │  ├─── D1                                                 bool                    True
    #    │  │     │  ├─── D2                                                 bool                    False
    #    │  │     │  └──┬ D3                                                 list                    
    #    │  │     │     └──┬ [0]                                             dict                    
    #    │  │     │        └─── D3a                                          int                     123
    #    │  │     └──┬ [1]                                                   dict                    
    #    │  │        ├─── D1                                                 bool                    True
    #    │  │        ├─── D2                                                 bool                    False
    #    │  │        └──┬ D3                                                 list                    
    #    │  │           └──┬ [0]                                             dict                    
    #    │  │              ├─── D3a                                          int                     456
    #    │  │              └─── D3b                                          str                     Something
    #    │  ├──┬ [3]                                                         dict                    
    #    │  │  ├─── E                                                        NoneType                None
    #    │  │  └─── FFF                                                      int                     666
    #    │  ├──┬ [4]                                                         dict                    
    #    │  │  └─── G                                                        int                     123
    #    │  ├──┬ [5]                                                         dict                    
    #    │  │  └──┬ G                                                        list                    
    #    │  │     ├─── [0]                                                   int                     1
    #    │  │     ├─── [1]                                                   int                     2
    #    │  │     ├─── [2]                                                   int                     3
    #    │  │     ├─── [3]                                                   int                     4
    #    │  │     ├─── [4]                                                   int                     5
    #    │  │     ├─── [5]                                                   int                     6
    #    │  │     ├─── [6]                                                   int                     7
    #    │  │     ├─── [7]                                                   int                     8
    #    │  │     ├─── [8]                                                   int                     9
    #    │  │     └─── [9]                                                   int                     10
    #    │  └──┬ [6]                                                         dict                    
    #    │     ├─── FFF                                                      float                   456.123
    #    │     └──┬ G                                                        list                    
    #    │        ├─── [0]                                                   int                     2
    #    │        ├─── [1]                                                   int                     3
    #    │        ├─── [2]                                                   int                     4
    #    │        ├─── [3]                                                   int                     5
    #    │        ├─── [4]                                                   int                     6
    #    │        └─── [5]                                                   int                     1
    #    └──┬ type_errors                                                    list                    
    #       ├─── [0]                                                         NoneType                None
    #       └─── [1]                                                         int                     123

    _field_names = OrderedSet()
    _fields = OrderedDict({})
    _added_fields_count = []
    _type_errors = []
    _list_out = _type_errors # These are aliases - because both started as lists of something; we don't know whether its a simple list of values or a list of records with invalid entries
    _records_out = []

    try:
        if (records):
            for _id, _record in enumerate(records):
                if (isinstance(_record, dict)):
                    _existing_field_count = len(_field_names)
                    _dict_keys = list(_record.keys())
                    _field_names = _field_names | OrderedSet(_dict_keys)
                    _added_fields_count.append(len(_field_names) - _existing_field_count)

                    # Record all the _py_dtypes involved
                    for _field in _field_names:
                        # This function self-protects against fields having invalid names - but these should have been done at data level before calling this function.
                        _clean_field_name = load_datawarehouse.data.clean_field_key(_field)

                        # See if we have had this field before
                        if (not _field in _fields):
                            _fields[_clean_field_name] = OrderedSet()

                        # Using .get() instead of _field in _record allows us to catch _record[_field] that are Nones; which should not be added to _py_dtypes.
                        if (_record.get(_field, None) is not None):
                            if (isinstance(
                                _record[_field],
                                (
                                    dict,
                                    tuple,
                                    list,
                                    np.ndarray,
                                    pd.DataFrame,
                                    pd.Series,
                                )
                            )):
                                if (isinstance(
                                    _record[_field],
                                    (
                                        pd.DataFrame,
                                    )
                                )):
                                    _obj = _record[_field].to_dict(orient="records")
                                else:
                                    _obj = _record[_field]

                                _deconstructed = deconstruct_records(_obj)

                                if (hasattr(_deconstructed, "fields")):
                                    # Its a RecordFields
                                    _type = (_deconstructed.fields, )
                                else:
                                    # Its a simple List
                                    _type = (_deconstructed.types, )
                            else:
                                _type = (type(_record[_field]), )

                            _fields[_clean_field_name] = _fields[_clean_field_name] | OrderedSet(_type)

                    _records_out.append(
                        _record,
                    )
                else:
                    # What if this is a list??
                    _list_out.append(
                        _record
                    )

            _total_count = _id+1
        else:
            _total_count = 0


        # Determine if this is a Records or a List

        if (not _records_out):
            # Its a List
            return DeconstructedList(
                types = ListField(
                            # Force a generator into a tuple so that it becomes immutable; allows set() to be used on it
                            OrderedSet(
                                [type(_list_item) for _list_item in _list_out]
                            ),
                ),
                list = _list_out,
                type_errors = _records_out,
            )

        else:

            if (_added_fields_count):
                _void = _added_fields_count.pop(0)
            _factor_of_records_adding_fields = np.count_nonzero(np.array(_added_fields_count))/_total_count

            # This function self-protects against fields having invalid names - but these should have been done at data level before calling this function.
            return DeconstructedRecords(
            fields=namedtuple("RecordFields",
                    field_names=( load_datawarehouse.data.clean_field_key(_key) for _key in _fields.keys() ))(
                    **{
                    load_datawarehouse.data.clean_field_key(_key):tuple(_py_dtypes) \
                        for _key, _py_dtypes in \
                            zip(_fields, _fields.values())
                    }
                ),
            factor_of_records_adding_fields=_factor_of_records_adding_fields,
            records=_records_out,
            type_errors=_type_errors,
            )

    except TypeError as e:
        raise TypeError(f"Provided records are not iterable; expected Iterable[Dict], {type(records).__name__} found.")
