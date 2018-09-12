import attr


class ProtocolError(Exception):
    pass


class LimitBreached(ProtocolError):
    pass


class CodingError(ProtocolError):
    pass


class UnexpectedPong(ProtocolError):
    pass


class UnexpectedAck(ProtocolError):
    pass


class NetworkError(Exception):
    pass


class IncompleteMessage(NetworkError):
    pass


class PingTimeout(NetworkError):
    pass


@attr.s(cmp=False)
class DataError(Exception):
    """Can be thrown from data handlers to return an ERROR instead of an ACK"""
    error_code = attr.ib(default=None)
    error = attr.ib(default=None)
