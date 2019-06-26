# -*- coding: utf-8 -*-


class BaseEnhancement(object):
    """ Enhancements take a dictionary object and modify it in some way provide enhancement. """

    def __init__(self, rule):
        self.rule = rule


class MatchEnhancement(BaseEnhancement):
    """ Enhancements take a match dictionary object and modify it in some way to
    enhance an alert. These are specified in each rule under the match_enhancements option.
    Generally, the key value pairs in the match module will be contained in the alert body. """

    def process(self, match):
        """
        Modify the contents of match, a dictionary, in some way
        :raises: DropMatchException
        """
        raise NotImplementedError()


class AlertEnhancement(BaseEnhancement):
    """ Enhancements take an alert dictionary object and modify it in some way to
    enhance an alert. These are specified in each rule under the alert_enhancements option. """

    def process(self, alert):
        """ Modify the contents of alert, a dictionary, in some way """
        raise NotImplementedError()


class DropException(Exception):
    """ Reactor will drop an alert if this exception type is raised by an enhancement """
    pass
