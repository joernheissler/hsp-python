class ProtocolError(Exception):
    pass


class LimitBreached(ProtocolError):
    pass


class CodingError(ProtocolError):
    pass


class IncompleteMessage(ProtocolError):
    pass


class UnexpectedPong(ProtocolError):
    pass


class UnexpectedAck(ProtocolError):
    pass
