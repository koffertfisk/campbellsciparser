"""Common tools for implementing Campbell Scientific data logger parsers. """

from pytz.exceptions import UnknownTimeZoneError


class TimeColumnValueError(ValueError):
    pass


class TimeParsingError(ValueError):
    pass


class UnknownPytzTimeZoneError(UnknownTimeZoneError):
    pass

