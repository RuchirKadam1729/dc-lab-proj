from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
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
    __slots__ = ()
    def __init__(self) -> None: ...

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
