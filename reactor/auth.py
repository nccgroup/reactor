
class Auth(object):

    def __call__(self, host: str, username: str, password: str):
        """ Return the authorization header.

        :param host: Elasticsearch host.
        :param username: Username used for authenticating the requests to Elasticsearch.
        :param password: Password used for authenticating the requests to Elasticsearch.
        """
        if username and password:
            return username + ':' + password
