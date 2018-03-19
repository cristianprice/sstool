from org.sstool.varint import decode_stream
from contextlib import contextmanager
import mmap


@contextmanager
def load_ss_file(path):
    with open(path, "r+") as f:
        try:
            map = mmap.mmap(f.fileno(), 0)
            yield map

        except Exception as e:
            print(e)

        finally:
            map.close()


def _read_one(stream):
    c = stream.read(1)
    if c == '':
        raise EOFError("Unexpected EOF while reading bytes")
    return c


def dump_bytes(path, bytes):
    with open(path, mode='wb') as f:
        f.write(bytes)


def read_short(mf, byteorder='big', signed=False):
    bts = read_bytes(mf, 2)
    return int.from_bytes(bts, byteorder=byteorder, signed=signed)


def read_int32(mf, byteorder='big', signed=False):
    bts = read_bytes(mf, 4)
    return int.from_bytes(bts, byteorder=byteorder, signed=signed)


def read_int64(mf, byteorder='big', signed=False):
    bts = read_bytes(mf, 8)
    return int.from_bytes(bts, byteorder=byteorder, signed=signed)


def read_bytes(mf, size=10):
    c = mf.read(size)
    if c == '':
        raise EOFError("Unexpected EOF while reading bytes")
    return c


def read_varint(mf):
    return decode_stream(mf)
