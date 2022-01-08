from requests import exceptions

class WarehouseAPIFaked(RuntimeError):
    def __bool__(self):
        return False
    __nonzero__ = __bool__

class WarehouseAPINotInstalled(WarehouseAPIFaked):
    pass

class WarehouseAPICredentialsMissing(WarehouseAPIFaked):
    pass

class WarehouseInvalidInput(RuntimeError):
    def __bool__(self):
        return False
    __nonzero__ = __bool__

class WarehouseTableGenericError(RuntimeError):
    def __init__(
        self,
        *args,
        exception:Exception,
        **kwargs,
    )->None:
        super().__init__(
            *args,
            **kwargs,
        )
        
        self.exception = exception

    def __bool__(self):
        return False
    __nonzero__ = __bool__
        

class WarehouseAccessDenied(RuntimeError):
    def __bool__(self):
        return False
    __nonzero__ = __bool__

class WarehouseTableNotFound(RuntimeError):
    def __bool__(self):
        return False
    __nonzero__ = __bool__

class WarehouseTableRowsInvalid(ValueError):
    def __bool__(self):
        return False
    __nonzero__ = __bool__

class WarehouseRowOversize(RuntimeError):
    def __bool__(self):
        return False
    __nonzero__ = __bool__