import msgpack


def serialize(data: dict) -> bytes:
    """Serialize a dict to msgpack bytes."""
    return msgpack.packb(data, use_bin_type=True)


def deserialize(data: bytes) -> dict:
    """Deserialize msgpack bytes to a dict."""
    return msgpack.unpackb(data, raw=False)