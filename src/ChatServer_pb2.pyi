import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ChatMessage(_message.Message):
    __slots__ = ("msg_id", "sender_id", "recipient_id", "payload", "v_clock", "date_time")
    class VClockEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    MSG_ID_FIELD_NUMBER: _ClassVar[int]
    SENDER_ID_FIELD_NUMBER: _ClassVar[int]
    RECIPIENT_ID_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    V_CLOCK_FIELD_NUMBER: _ClassVar[int]
    DATE_TIME_FIELD_NUMBER: _ClassVar[int]
    msg_id: str
    sender_id: str
    recipient_id: str
    payload: _containers.RepeatedScalarFieldContainer[str]
    v_clock: _containers.ScalarMap[str, int]
    date_time: _timestamp_pb2.Timestamp
    def __init__(self, msg_id: _Optional[str] = ..., sender_id: _Optional[str] = ..., recipient_id: _Optional[str] = ..., payload: _Optional[_Iterable[str]] = ..., v_clock: _Optional[_Mapping[str, int]] = ..., date_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ChatServerResponse(_message.Message):
    __slots__ = ("msg_id", "status_code", "payload", "v_clock", "date_time")
    class status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        DUP: _ClassVar[ChatServerResponse.status]
        DELIVERED_LOCAL: _ClassVar[ChatServerResponse.status]
        QUEUED_LOCAL: _ClassVar[ChatServerResponse.status]
        DELIVERED_REMOTE: _ClassVar[ChatServerResponse.status]
        QUEUED_REMOTE: _ClassVar[ChatServerResponse.status]
        QUEUED_FALLBACK: _ClassVar[ChatServerResponse.status]
    DUP: ChatServerResponse.status
    DELIVERED_LOCAL: ChatServerResponse.status
    QUEUED_LOCAL: ChatServerResponse.status
    DELIVERED_REMOTE: ChatServerResponse.status
    QUEUED_REMOTE: ChatServerResponse.status
    QUEUED_FALLBACK: ChatServerResponse.status
    class VClockEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    MSG_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_CODE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    V_CLOCK_FIELD_NUMBER: _ClassVar[int]
    DATE_TIME_FIELD_NUMBER: _ClassVar[int]
    msg_id: str
    status_code: ChatServerResponse.status
    payload: _containers.RepeatedScalarFieldContainer[str]
    v_clock: _containers.ScalarMap[str, int]
    date_time: _timestamp_pb2.Timestamp
    def __init__(self, msg_id: _Optional[str] = ..., status_code: _Optional[_Union[ChatServerResponse.status, str]] = ..., payload: _Optional[_Iterable[str]] = ..., v_clock: _Optional[_Mapping[str, int]] = ..., date_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
