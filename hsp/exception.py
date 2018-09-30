class InvalidOperation(Exception):
    """
    Raised when an operation is invalid in the current state.
    """


class QueueFull(Exception):
    """
    Raised when a queue is full and more items cannot be added.
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


class DuplicateKeyError(Exception):
    """
    Key is allowed only once.
    """
