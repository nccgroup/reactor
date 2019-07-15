
class ReactorException(Exception):
    pass


class ConfigException(ReactorException):
    pass


class QueryException(ReactorException):
    def __init__(self, query: str, *args, **kwargs):  # real signature unknown
        self.__init__(*args, **kwargs)
        self.query = query
