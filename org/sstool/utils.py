from org.sstool.varint import decode_stream


def read_short(mf, byteorder='big', signed=False):
    bts = mf.read(2)
    return int.from_bytes(bts, byteorder=byteorder, signed=signed)


def read_int32(mf, byteorder='big', signed=False):
    bts = mf.read(4)
    return int.from_bytes(bts, byteorder=byteorder, signed=signed)


def read_int64(mf, byteorder='big', signed=False):
    bts = mf.read(8)
    return int.from_bytes(bts, byteorder=byteorder, signed=signed)


def read_bytes(mf, size=10):
    return mf.read(size)


def read_varint(mf):
    return decode_stream(mf)


def read_deletion_time(mf):
    return read_int32(mf), read_int64(mf)
