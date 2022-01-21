from abc import abstractmethod, abstractclassmethod

class DataWarehouseMeta(type):
    """
    Metaclass.

    Currently unused.
    """
    pass

class DataWarehouse(metaclass=DataWarehouseMeta):
    """
    Abstract Class for a DataWarehouse.

    More accurately, one instance of a DataWarehouse class corresponds to a the Table(SQL/BigQuery) or Database(Redshift) level of data structure;
    to work on multiple Tables, you will need multiple instances of DataWarehouse.

    The Abstract Class itself cannot be used; use the subclasses from each platform.
    """

    @abstractclassmethod
    def get(self):
        pass

    @abstractclassmethod
    def new(self):
        pass
    
    @abstractmethod
    def touch(self):
        pass

    @abstractmethod
    def fetch(self):
        pass

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def update(self):
        pass

    @abstractmethod
    def delete(self):
        pass

class DataWarehouseUnavailable(metaclass=DataWarehouseMeta):
    """
    If the DataWarehouse is not available for some reason, an instance of this class will be returned from any of the @classmethod constructors.

    It throws an exception immediate upon init. Its purpose is academic currently.
    """
    exception = Exception("Generic Exception for unavailable API.")

    def __init__(self, exception:Exception, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exception = exception

        raise self.exception

    def __getattr__(self, attr):
        raise self.exception



class APITypesMetaclass(type):
    """
    Metaclass that allows api_types to pass all type hint checks even if API is missing.

    Each platform will have its own list of types, which is a class object with APITypeMetaclass as metaclass.
    No instances of this class will be used, hence the metaclass requiring a dunder __getattr__ function instead of the class itself.
    """
    def __getattr__(self, attr):
        return type(None)