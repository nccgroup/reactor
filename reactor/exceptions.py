
class ReactorException(Exception):
    """ A general exception raised by Reactor. Also the base class for other exceptions. """
    pass


class ConfigException(ReactorException):
    """ Reactor raises ConfigExceptions when loaded configuration files are invalid. """
    pass


class QueryException(ReactorException):
    """
    Reactor raises QueryExceptions when an Elasticsearch query fails.

    :param query: Search body sent to Elasticsearch
    """
    def __init__(self, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query


class ClusterException(ReactorException):
    """ Reactor raises ClusterExceptions when an issue occurs between cluster nodes. """
    pass
