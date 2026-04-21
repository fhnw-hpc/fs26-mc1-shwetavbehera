import msgpack


def serialize(data: dict) -> bytes:
    return msgpack.packb(data, use_bin_type=True)


def deserialize(data: bytes) -> dict:
    return msgpack.unpackb(data, raw=False)