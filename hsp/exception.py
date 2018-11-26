import attr
from typing import ByteString, Optional


class ProtocolError(Exception):
    """
    Peer violated the protocol.
    """


class LimitBreached(ProtocolError):
    """
    Peer sent some value that was too large.
    """


class IncompleteMessage(ProtocolError):
    """
    Partial message received.
    """


class UnexpectedPong(ProtocolError):
    """
    Peer sent a PONG without us sending a PING.
    """


class UnexpectedAck(ProtocolError):
    """
    Peer sent an ACK / ERROR without us sending a DATA_ACK with that message id.
    """


class UnknownCommand(ProtocolError):
    """
    Peer sent an unknown command.
    """


@attr.s(cmp=False)
class DataError(Exception):
    """
    Peer replied with an Error
    """

    code = attr.ib(type=Optional[int])
    payload = attr.ib(type=Optional[ByteString])
