"""Common tools for implementing Campbell Scientific data logger parsers. """

from pytz.exceptions import UnknownTimeZoneError


class DataTypeError(TypeError):
    pass


class TimeColumnValueError(ValueError):
    pass


class TimeConversionException(Exception):
    pass


class TimeParsingException(ValueError):
    pass


class TimeZoneAlreadySet(ValueError):
    pass


class UnknownPytzTimeZoneError(UnknownTimeZoneError):
    pass


class UnsupportedTimeFormatError(ValueError):
    pass
