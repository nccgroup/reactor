
class ReactorException(Exception):
    """ A general exception raised by Reactor. Also the base class for other exceptions. """
    pass


class ConfigException(ReactorException):
    """ Reactor raises ConfigExceptions when loaded configuration files are invalid. """
    @staticmethod
    def from_jsonschema_validation_error(filename, error):
        message = f''
        if error.validator == 'oneOf':
            if not error.path:
                message += f"""Config must have one and only one of {error.schema['oneOf']}"""
            else:
                message += f"""property '{'.'.join(error.path)}' must be one and only one of {error.schema['oneOf']}"""
        elif error.validator == 'anyOf':
            message += f"""property '{'.'.join(error.path)}' must be any of {error.schema['anyOf']}"""
        elif error.validator == 'type':
            message += f"""property '{'.'.join(error.path)}' with value {error.message}"""
        elif error.validator == 'required':
            message += f"""{error.message}"""
        else:
            message += f"""{error.message} - {error.validator}"""
        return ConfigException(message, locator=filename)

    def __init__(self, message, *args, locator=None, **kwargs):
        """
        :param message: Description of the cause
        :param locator: Locator of the configuration
        """
        args = (message, *args, locator) if locator else (message, *args)
        super().__init__(*args, **kwargs)
        self.message = message or args[0]
        self.filename = locator

    def __str__(self):
        string = self.message
        if self.filename:
            string = f'Invalid configuration "{self.filename}" {string}'
        return string


class QueryException(ReactorException):
    """
    Reactor raises QueryExceptions when an Elasticsearch query fails.

    :param query: Search body sent to Elasticsearch
    """
    def __init__(self, *args, query, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
