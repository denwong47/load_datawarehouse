import os, sys
import unittest
from io import StringIO, BytesIO
from typing import Union

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from load_datawarehouse.data import chunks, json_size

class TestCaseFileIOError(IOError):
    def __bool__(self):
        return False
    __nonzero__ = __bool__

class TestCasePickleCorrupted(RuntimeError):
    def __bool__(self):
        return False
    __nonzero__ = __bool__


def read_file(
    path:str,
    output:type=str,
)->Union[str, bytes]:
    try:
        with open(path, f"r{'b' if output is bytes else ''}") as _fHnd:
            _return = _fHnd.read()
    except Exception as e:
        _return = TestCaseFileIOError(str(e))

    return _return

_test_data = None

def setUpModule() -> None:
    global _test_data
    
    _file_names = [
        "articles.json",
        "sku_specsheet_combined.json",
    ]

    _test_data = {
        _file_name: \
            read_file(
                TestBaseClass.get_testdata_path(f"{_file_name}"),
                output=bytes if (os.path.splitext(_file_name)[1] in [".pickle",]) else str,
            ) for _file_name in _file_names
    }


class TestBaseClass(unittest.TestCase):

    @classmethod
    def get_testdata_path(cls, filename:str)->str:
        return os.path.join(cls.get_testdata_dir(), filename)

    @classmethod
    def get_testdata_dir(cls):
        return os.path.join(
            os.path.dirname(sys.argv[0]),
            "data/"
            )

    def get_testdata(
        self,
        filename:str,
        file_like:bool=False,
    )->str:
        global _test_data
        _data = _test_data.get(filename, None)

        _type_switch = {
            bytes: BytesIO,
            str: StringIO,
            type(None): lambda obj: None,
        }
        
        if (file_like):
            _data = _type_switch.get(
                type(_data),
                lambda obj: obj,
            )(_data)
        
        return _data

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()

        setUpModule()

    @classmethod
    def tearDownClass(cls) -> None:
        super().tearDownClass()

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def conduct_tests(
        self,
        func,
        tests:dict,
        ):

        for _test in tests:
            if (issubclass(_test["answer"], Exception) if (isinstance(_test["answer"], type)) else False):
                with self.assertRaises(Exception) as context:
                    _return = func(
                        **_test["args"]
                    )
                    if (isinstance(_return, Exception)):
                        raise _return

                self.assertTrue(isinstance(context.exception, _test["answer"]))
            elif (isinstance(_test["answer"], type)):
                self.assertTrue(isinstance(func(**_test["args"]), _test["answer"]))
            elif (isinstance(_test["answer"], np.ndarray)):
                if (_test["answer"].dtype in (
                    np.float_,
                    np.float16,
                    np.float32,
                    np.float64,
                    np.float128,
                    np.longfloat,
                    np.half,
                    np.single,
                    np.double,
                    np.longdouble,
                )):
                    _assertion = np.testing.assert_allclose
                else:
                    _assertion = np.testing.assert_array_equal

                _assertion(
                    func(
                        **_test["args"]
                    ),
                    _test["answer"],
                )
            elif (isinstance(_test["answer"], pd.DataFrame)):
                _assertion = assert_frame_equal

                _assertion(
                    func(
                        **_test["args"]
                    ),
                    _test["answer"],
                )
            else:
                self.assertEqual(
                    func(
                        **_test["args"]
                    ),
                    _test["answer"],
                )

        
class TestLoadDataWarehouse(TestBaseClass):
    def test_chunks(self, show_process:bool=True):
        # A random set of data with varying but increase-treading size
        _data = [
            {
                "a":_id*10,
                "b something":{ # some invalid data keys
                    "b 1":True,
                    "b.2":{
                        _id:"Something",
                        "List":list(range(10)),
                        "Dicts":{
                            f"Item #{_}":_ for _ in range(_id*5 * (_id % 3 +1))
                        }
                    }
                }
            } for _id in range(1000)
        ]

        # _dataframe = pd.DataFrame(_data)
        _dataframe = _data

        if show_process: print (f"Whole data has {len(_dataframe)} rows, totalling {json_size(_dataframe)} bytes.")

        _cum_row = 0
        _cum_size = 0
        _size_limit = 10*2**20

        _reconstructed = []
        for _id, _chunk in enumerate(chunks(_dataframe, size_limit=_size_limit)):
            _cum_row += len(_chunk)
            _cum_size += json_size(_chunk)
            _reconstructed += _chunk

            if show_process: print (f"Chunk #{_id:4d} has {len(_chunk)} rows, totalling {json_size(_chunk)} bytes ({json_size(_chunk)/_size_limit:%}); Cumulative {_cum_row} rows and {_cum_size} bytes.")

        self.assertListEqual(_reconstructed, _data)

    
if __name__ == "__main__":
    unittest.main()