import attr


class InvalidOperation(Exception):
    """
    Raised when an operation is invalid in the current state.
    """


class ProtocolError(Exception):
    """
    Peer violated the protocol.
    """


class LimitBreached(ProtocolError):
    """
    Peer sent some value that was too large.
    """


class CodingError(ProtocolError):
    """
    Error when decoding some bytes sent by the peer.
    """


class UnexpectedPong(ProtocolError):
    """
    Peer sent a PONG without us sending a PING.
    """


class UnexpectedAck(ProtocolError):
    """
    Peer sent an ACK / ERROR without us sending a DATA_ACK with that message id.
    """


class NetworkError(Exception):
    """
    Connection broke because of some network error.
    """


class IncompleteMessage(NetworkError):
    """
    Hit EOF while in the middle of a message.
    """


class PingTimeout(NetworkError):
    """
    Peer didn't respond to a PING fast enough.
    """


@attr.s(cmp=False)
class DataError(Exception):
    """
    Can be thrown from data handlers to return an ERROR instead of an ACK.
    """

    error_code = attr.ib(default=None)
    error = attr.ib(default=None)
