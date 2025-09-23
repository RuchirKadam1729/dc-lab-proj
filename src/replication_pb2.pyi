from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class VectorClock(_message.Message):
    __slots__ = ("clock",)
    class ClockEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    CLOCK_FIELD_NUMBER: _ClassVar[int]
    clock: _containers.ScalarMap[str, int]
    def __init__(self, clock: _Optional[_Mapping[str, int]] = ...) -> None: ...

class ChatMessage(_message.Message):
    __slots__ = ("id", "sender", "body", "ts", "vc")
    ID_FIELD_NUMBER: _ClassVar[int]
    SENDER_FIELD_NUMBER: _ClassVar[int]
    BODY_FIELD_NUMBER: _ClassVar[int]
    TS_FIELD_NUMBER: _ClassVar[int]
    VC_FIELD_NUMBER: _ClassVar[int]
    id: str
    sender: str
    body: str
    ts: int
    vc: VectorClock
    def __init__(self, id: _Optional[str] = ..., sender: _Optional[str] = ..., body: _Optional[str] = ..., ts: _Optional[int] = ..., vc: _Optional[_Union[VectorClock, _Mapping]] = ...) -> None: ...

class ReplicateRequest(_message.Message):
    __slots__ = ("msg", "origin_node")
    MSG_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_NODE_FIELD_NUMBER: _ClassVar[int]
    msg: ChatMessage
    origin_node: str
    def __init__(self, msg: _Optional[_Union[ChatMessage, _Mapping]] = ..., origin_node: _Optional[str] = ...) -> None: ...

class ReplicateResponse(_message.Message):
    __slots__ = ("ok", "reason")
    OK_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    reason: str
    def __init__(self, ok: bool = ..., reason: _Optional[str] = ...) -> None: ...
