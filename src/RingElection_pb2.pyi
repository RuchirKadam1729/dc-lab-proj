from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ElectionMessage(_message.Message):
    __slots__ = ("originator_id", "node_ids", "priorities")
    ORIGINATOR_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_IDS_FIELD_NUMBER: _ClassVar[int]
    PRIORITIES_FIELD_NUMBER: _ClassVar[int]
    originator_id: str
    node_ids: _containers.RepeatedScalarFieldContainer[str]
    priorities: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, originator_id: _Optional[str] = ..., node_ids: _Optional[_Iterable[str]] = ..., priorities: _Optional[_Iterable[int]] = ...) -> None: ...

class LeaderMessage(_message.Message):
    __slots__ = ("leader_id", "originator_id")
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    ORIGINATOR_ID_FIELD_NUMBER: _ClassVar[int]
    leader_id: str
    originator_id: str
    def __init__(self, leader_id: _Optional[str] = ..., originator_id: _Optional[str] = ...) -> None: ...

class RequestMessage(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ResponseMessage(_message.Message):
    __slots__ = ("priority",)
    PRIORITY_FIELD_NUMBER: _ClassVar[int]
    priority: int
    def __init__(self, priority: _Optional[int] = ...) -> None: ...
