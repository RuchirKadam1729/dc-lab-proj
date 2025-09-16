from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class RequestMessage(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ResponseMessage(_message.Message):
    __slots__ = ("priority",)
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    priority: int
    def __init__(self, priority: _Optional[int] = ...) -> None: ...

class ElectionMessage(_message.Message):
    __slots__ = ("priors",)
    PRIORS_FIELD_NUMBER: _ClassVar[int]
    priors: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, priors: _Optional[_Iterable[int]] = ...) -> None: ...

class CandidateMessage(_message.Message):
    __slots__ = ("priority",)
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    priority: int
    def __init__(self, priority: _Optional[int] = ...) -> None: ...

class LeaderMessage(_message.Message):
    __slots__ = ("leader_id",)
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    leader_id: str
    def __init__(self, leader_id: _Optional[str] = ...) -> None: ...
