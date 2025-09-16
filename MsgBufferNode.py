from protos.ChatServer_pb2 import ChatMessage
class MsgBufferNode:
    def __init__(self, val: ChatMessage):
        self.val = val

    @staticmethod  # below is gpt-generated vclock ordering
    def _vc_cmp(a: dict, b: dict) -> int:
        """Return -1 if a < b, 1 if a > b, 0 if concurrent/equal."""
        keys = sorted(set(a.keys()) | set(b.keys()))
        pairs = ((a.get(k, 0), b.get(k, 0)) for k in keys)
        a_le_b = all(x <= y for x, y in pairs)

        pairs = ((a.get(k, 0), b.get(k, 0)) for k in keys)
        b_le_a = all(x >= y for x, y in pairs)

        pairs = ((a.get(k, 0), b.get(k, 0)) for k in keys)
        a_lt_b = any(x < y for x, y in pairs)
        pairs = ((a.get(k, 0), b.get(k, 0)) for k in keys)
        b_lt_a = any(x > y for x, y in pairs)

        if a_le_b and a_lt_b:
            return -1
        if b_le_a and b_lt_a:
            return 1
        return 0

    # me actually implementing said logic
    def __lt__(self, other: "MsgBufferNode") -> bool:
        a_vc = dict(getattr(self.val, "v_clock", {}) or {})
        b_vc = dict(getattr(other.val, "v_clock", {}) or {})
        cmp = self._vc_cmp(a_vc, b_vc)
        if cmp != 0:
            return cmp == -1

        # tie-breaker for equal clocks
        a_key = (
            getattr(self.val, "sender_id", ""),
            getattr(self.val, "recipient_id", ""),
            tuple(getattr(self.val, "payload", ())),
        )
        b_key = (
            getattr(other.val, "sender_id", ""),
            getattr(other.val, "recipient_id", ""),
        )
        return a_key < b_key
