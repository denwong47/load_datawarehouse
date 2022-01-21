import os, sys

import json
import random
import re
from typing import Any, Dict, Iterable, List, Union

import pandas as pd

from load_datawarehouse.exceptions import WarehouseRowOversize


def clean_field_key(key:str)->str:
    """
    Substitute all prohibited characters in field names with an underscore.
    Probited characters are defined as any non-word/digit characters.

    As the field names will be used in a NamedTuple, which goes by the syntax:
        NamedTupleClass(field_name1=something, field_name2=something...)
    all field names have to be allowable in python syntax as well.
    """
    if (not isinstance(key, str)):
        key = str(key)
        
    _prohibited = re.compile(r"\W")
    return _prohibited.sub("_", key)


def clean_keys(obj:Any)->Any:
    """
    Universal function for cleaning the keys/columns of multiple types of objects.
    """
    _default = lambda obj: obj

    _type_switch = {
        dict: clean_dict_keys,
        list: clean_list_keys,
        pd.DataFrame: clean_dataframe_keys,
    }

    return _type_switch.get(
        type(obj),
        _default
    )(
        obj
    )

def clean_dict_keys(dictobj:Dict[Any, Any])->Dict[str, Any]:
    """
    As per clean_field_key(), but takes a dictionary as arguments.
    """
    if (isinstance(dictobj, dict)):
        return {
            clean_field_key(_key): clean_keys(_value) \
                for _key, _value in zip(dictobj, dictobj.values())
        }
    else:
        return clean_keys(dictobj)

def clean_list_keys(listobj:List[Any])->List[Any]:
    """
    As per clean_field_key(), but takes an iterable as arguments.
    """
    if (isinstance(listobj, (list, tuple))):
        return [
            clean_keys(_item) for _item in listobj
        ]
    else:
        return clean_keys(listobj)

def clean_dataframe_keys(dataframe:pd.DataFrame)->pd.DataFrame:
    """
    As per clean_field_key(), but takes a Pandas Dataframe as arguments.

    This clean up both the columns of the dataframe,
    as well as values of any columns that are Objects in dtype.

    e.g. if one column contains dicts as its values, this function will cycle through them too.
    """
    dataframe = clean_dataframe_columns(dataframe)
    dataframe = clean_dataframe_values(dataframe)
    return dataframe # We are muting pd.DataFrame in place so returning this or not does not actually matter



def clean_dataframe_values(dataframe:pd.DataFrame)->pd.DataFrame:
    """
    Clean up the values of any columns that are Objects in dtype.

    e.g. if one column contains dicts as its values, this function will clean the keys of those.
    """
    _object_columns = dataframe.select_dtypes(include='object')

    def clean_row(row:pd.Series)->pd.Series:
        return row.apply(func=clean_keys)

    _object_columns = _object_columns.apply(
        clean_row,
        axis=1,
    )

    dataframe = dataframe.copy()

    dataframe.update(
        other = _object_columns,
        join="left",
        overwrite=True,
    )

    return dataframe

def clean_dataframe_columns(dataframe:pd.DataFrame)->pd.DataFrame:
    """
    Clean up the name of all columns in a DataFrame.
    """
    _mapper = {
        _column:clean_field_key(_column) \
            for _column in dataframe.columns
    }
    return dataframe.rename(_mapper, inplace=False)


def prepare(
    data:Union[
        Iterable[Dict[str,str]],
        pd.DataFrame
    ],
):
    """
    Prepare data for use, such as :
    1. cleaning the keys
    2. turning the data into records (list of dicts)
    """
    data = clean_keys(data)
    if (isinstance(data, list)):
        pass
    elif (isinstance(data, pd.DataFrame)):
        data = clean_keys(data)
        data = data.to_dict(orient="records")
    
    return data

def json_size(
    data:Union[
        Iterable[Dict[str,str]],
        pd.DataFrame
    ],
):
    """
    Calculate the size of records or DataFrame in json format.
    
    Since JSON is a common format for data loading, which occasionally has size limits,
    this function allows JSON size to be checked prior to uploading.

    See chunks() function for a better solution to chunk up JSONs by size.
    """
    if (isinstance(data, pd.DataFrame)):
        return sys.getsizeof(
            data.to_json(
                path_or_buf=None, # Return as string,
                orient="records",
                default_handler=str,
                indent=0,
            )
        )
    elif (isinstance(data, list)):
        return sys.getsizeof(
            json.dumps(
                data,
                default=str,
            )
        )
    else:
        return sys.getsizeof(data)

def sample(
    data:Union[
        Iterable[Dict[str,str]],
        pd.DataFrame
    ],
    size:int,
):
    """
    Randomly sample a number of records from data.

    Primarily for internal use only in chunks().
    """

    if (isinstance(data, pd.DataFrame)):
        return data.sample(
            n=size,
            axis=0,
        )
    else:
        return random.sample(
            data,
            size,
        )

def subset(
    data:Union[
        Iterable[Dict[str,str]],
        pd.DataFrame
    ],
    start:int,
    size:int,
):
    """
    Return a specific range of records in data.

    Primarily for internal use only in chunks().
    """
    if (isinstance(data, pd.DataFrame)):
        return data.iloc[start:start+size, :]
    else:
        return data[start:start+size]

def chunks(
    data:Union[
        Iterable[Dict[str,str]],
        pd.DataFrame
    ],
    size_limit:int=20*(2**20), # 20MB is BigQuery's default JSON limit
    max_iteration:int=6,
):
    """
    Generator function to slice the data into chunks, guaranteed to be below a maximum size.

    This is actually a commutationally heavy function - we are essentially JSON serialising a LOT of combination of data in order to chunk them up in the right size.
    The benefit of this is to minimise the number of API calls as well as maximising network throughput.
    """

    sample_size = 10

    if (len(data) <= sample_size):
        yield data
    else:
        estimated_total_size = json_size(sample(
            data,
            sample_size
        )) * len(data) / sample_size

        chunk_length = max(1, int(estimated_total_size / size_limit))
        chunk_start = 0

        while (chunk_start < len(data)):
            # Bisecting to find the rough chunking point
            for _iteration in range(max_iteration+1):
                if (_iteration>0):
                    # Don't do this on the first iteration
                    existing_length = chunk_length
                    chunk_length = min(
                        (
                            chunk_length + \
                            max(
                                1,
                                int(chunk_length * size_limit / chunk_size),
                            )
                        )//2, # Average the existing chunk_length and the new one - which is the bisection method
                        len(data)-chunk_start, # this is quite essential - if the starting chunk_length vastly exceed the entire length of the data set, the subsequent procedure to "walk back" chunk_length will take AGES!
                    )

                    if (existing_length == chunk_length):
                        break

                chunk_size = json_size(
                    subset(
                        data,
                        start=chunk_start,
                        size=chunk_length,
                    )
                )

            # size_limit is a hard cap, so if the size exceeds it, we have to cut it back.
            if (chunk_size > size_limit):
                _oversize = True
                for chunk_length in range(chunk_length, 1, -1):
                    _chunk = subset(
                                data,
                                start=chunk_start,
                                size=chunk_length,
                            )
                    if (json_size(_chunk) <= size_limit):
                        _oversize = False
                        yield _chunk
                        break

                if (_oversize):
                    raise WarehouseRowOversize(
                        f"Row #{chunk_start} has a size of {json_size(_chunk):d}, which exceeds size limit of {size_limit:,d} bytes."
                    )
            else:
                yield subset(
                                data,
                                start=chunk_start,
                                size=chunk_length,
                            )
            
            # move the start index
            chunk_start += chunk_length
        
