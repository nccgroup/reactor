import datetime
import logging
import os

import jsonschema
import yaml
from jsonschema.compat import (
    urlopen,
    urlsplit,
)


def yaml_schema(validator, schema_file: str, relative_file: str = None):
    """
    :param validator: Validator to be initiated
    :param schema_file: Path to the schema file
    :param relative_file: (optional) File from which `schema_file` is relative
    """
    if relative_file is not None:
        schema_file = os.path.join(os.path.dirname(relative_file), schema_file)
    base_uri = 'file://%s/' % os.path.dirname(schema_file)
    schema = yaml.safe_load(open(schema_file))
    return validator(schema, resolver=YamlRefResolver(base_uri, schema),
                     format_checker=jsonschema.draft7_format_checker)


def extend_with_default(validator_class):
    # validate_properties = validator_class.VALIDATORS['properties']

    def resolve(validator, sub_schema):
        while '$ref' in sub_schema:
            sub_schema = validator.resolver.resolve(sub_schema['$ref'])[1]

        for complex in ['anyOf', 'allOf', 'oneOf']:
            for index, choice in enumerate(sub_schema.get(complex, [])):
                while '$ref' in choice:
                    sub_schema[complex][index] = choice = validator.resolver.resolve(choice['$ref'])[1]

        return sub_schema

    def convert_to(sub_schema, instance, prop):
        if 'convert_to' in sub_schema:
            if sub_schema['convert_to'] == 'timedelta' and not isinstance(instance[prop], datetime.timedelta):
                instance[prop] = None if instance[prop] is None else datetime.timedelta(**instance[prop])
            if sub_schema['convert_to'] == 'log_level' and not isinstance(instance[prop], int):
                instance[prop] = logging.getLevelName(instance[prop])

    class ConvertToTypeChecker(jsonschema.TypeChecker):
        def is_type(self, instance, node_type):
            if node_type == 'timedelta':
                return isinstance(instance, datetime.timedelta)
            else:
                return jsonschema.Draft7Validator.TYPE_CHECKER.is_type(instance, node_type)

    def set_defaults(validator, properties, instance, schema):
        # Set default values
        for prop, sub_schema in properties.items():
            sub_schema = resolve(validator, sub_schema)
            if 'default' in sub_schema and isinstance(instance, dict):
                instance.setdefault(prop, sub_schema['default'])

        # Validate object properties
        if validator.is_type(instance, "object"):
            for prop, sub_schema in properties.items():
                if prop in instance:
                    if 'convert_to' in sub_schema or hasattr(sub_schema, 'convert_to'):
                        if sub_schema['convert_to'] == 'timedelta' and isinstance(instance[prop], datetime.timedelta):
                            continue
                        if sub_schema['convert_to'] == 'log_level' and isinstance(instance[prop], int):
                            continue
                    for error in validator.descend(instance[prop], sub_schema, path=prop, schema_path=property):
                        yield error

        # Convert fields
        for prop, sub_schema in properties.items():
            sub_schema = resolve(validator, sub_schema)
            if not isinstance(instance, dict) or prop not in instance:
                continue
            convert_to(sub_schema, instance, prop)
            for complex in ['anyOf', 'allOf', 'oneOf']:
                for choice in sub_schema.get(complex, []):
                    convert_to(choice, instance, prop)

    return jsonschema.validators.extend(validator_class, {'properties': set_defaults},
                                        type_checker=ConvertToTypeChecker())


SetDefaultsDraft7Validator = extend_with_default(jsonschema.Draft7Validator)


class YamlRefResolver(jsonschema.RefResolver):
    """
    A simple copy of the default `jsonschema.RefResolver` which switches out `json.loads` with `yaml.safe_load`.
    """
    def resolve_remote(self, uri):
        """
        Resolve a remote ``uri``.

        If called directly, does not check the store first, but after
        retrieving the document at the specified URI it will be saved in
        the store if :attr:`cache_remote` is True.

        .. note::

            If the requests_ library is present, ``jsonschema`` will use it to
            request the remote ``uri``, so that the correct encoding is
            detected and used.

            If it isn't, or if the scheme of the ``uri`` is not ``http`` or
            ``https``, UTF-8 is assumed.

        Arguments:

            uri (str):

                The URI to resolve

        Returns:

            The retrieved document

        .. _requests: https://pypi.org/project/requests/
        """
        try:
            import requests
        except ImportError:
            requests = None

        scheme = urlsplit(uri).scheme

        if scheme in self.handlers:
            result = self.handlers[scheme](uri)
        elif scheme in [u"http", u"https"] and requests:
            # Requests has support for detecting the correct encoding of
            # json over http
            result = requests.get(uri).json()
        else:
            # Otherwise, pass off to urllib and assume utf-8
            with urlopen(uri) as url:
                result = yaml.safe_load(url.read().decode("utf-8"))

        if self.cache_remote:
            self.store[uri] = result
        return result
