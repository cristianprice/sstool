# TODO Make it more efficient
from org.sstool.utils import _read_one


def count_leading_bits(b, t='0'):
    count = 0
    seq = format(b, '#010b')[2:]
    for c in seq:
        if c == t:
            count += 1
        else:
            break

    seq = seq[(count + 1):]
    val = int('0b' + seq, 0)

    return count, bytes([val])


def decode_stream(stream):
    return read_unsigned_vint(stream)


def read_unsigned_vint(stream, byteorder='big'):
    first_byte = ord(_read_one(stream))
    if first_byte == 0:
        return first_byte

    size, retval = count_leading_bits(first_byte, '1')
    retval = [retval]

    for c in range(size):
        retval.append(_read_one(stream))

    r = int.from_bytes(b''.join(retval), byteorder=byteorder, signed=False)
    return r
