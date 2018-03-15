import mmap
from contextlib import contextmanager
from org.sstool.utils import read_short
from org.sstool.utils import read_int32
from org.sstool.utils import read_int64
from org.sstool.utils import read_bytes
from org.sstool.utils import read_varint
from io import BytesIO
from org.sstool.utils import dump_bytes

MAX_BE32 = 0x7FFFFFFF
MIN_BE64 = 0x8000000000000000

# Bound kinds
EXCL_END_BOUND = 0
INCL_START_BOUND = 1
EXCL_END_INCL_START_BOUNDARY = 2
STATIC_CLUSTERING = 3
CLUSTERING = 4
INCL_END_EXCL_START_BOUNDARY = 5
INCL_END_BOUND = 6
EXCL_START_BOUND = 7

# Cell flags
IS_DELETED_MASK = 0x01,  # Whether the cell is a tombstone or not.
IS_EXPIRING_MASK = 0x02,  # Whether the cell is expiring.
HAS_EMPTY_VALUE_MASK = 0x04,  # Whether the cell has an empty value. This will be the case for a tombstone in particular.
USE_ROW_TIMESTAMP_MASK = 0x08,  # Whether the cell has the same timestamp as the row this is a cell of.
USE_ROW_TTL_MASK = 0x10,  # Whether the cell has the same TTL as the row this is a cell of.

# Cluster block header
EMPTY = 100
NULL = 1


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


def parse_index_file(path):
    with load_ss_file(path) as mf:
        while True:
            key_length = read_short(mf)
            if key_length == 0:
                break

            key = read_bytes(mf, key_length)
            position = read_varint(mf)
            promoted_idx_len = read_varint(mf)

            promoted_index = b''
            if promoted_idx_len != 0:
                promoted_index = parse_promoted_index(read_bytes(mf, promoted_idx_len))

            yield key_length, key, position, promoted_idx_len, promoted_index


def parse_promoted_index(promoted_index):
    # dump_bytes('/tmp/promoted_index', promoted_index)
    pi_bytes = BytesIO(promoted_index)

    deletion_time = parse_deletion_time(pi_bytes)
    if deletion_time == (MAX_BE32, MIN_BE64):
        print('No change')

    promoted_index_blocks_count = read_varint(pi_bytes)

    promoted_index_blocks = []
    for pib in range(promoted_index_blocks_count):
        # Iterate over the blocks.
        promoted_index_blocks.append(read_promoted_index_block(pi_bytes))

    return deletion_time, promoted_index_blocks


def read_promoted_index_block(stream):
    first_name = read_clustering_prefix(stream)
    last_name = read_clustering_prefix(stream)

    return first_name, last_name


def read_clustering_prefix(stream):
    kind = ord(stream.read(1))
    size = 0

    if kind == CLUSTERING:
        clustering_block = read_clustering_block(stream)
        pass
    elif kind == EXCL_END_BOUND:
        size = stream.read(1)
        clustering_block = read_clustering_block(stream)
        pass
    elif kind == INCL_START_BOUND:
        size = stream.read(1)
        clustering_block = read_clustering_block(stream)
        pass
    elif kind == EXCL_END_INCL_START_BOUNDARY:
        size = stream.read(1)
        clustering_block = read_clustering_block(stream)
        pass
    pass


def read_clustering_block(stream):
    clustering_block_header_val = parse_block_header(read_varint(stream))
    cell = parse_simple_cell(stream)

    return clustering_block_header_val, cell


def parse_simple_cell(stream):
    flags = ord(stream.read(1))

    return flags


def parse_block_header(block_header):
    seq = format(block_header, '#010b')[2:]
    if seq[0] == '1':
        return NULL
    elif seq[-1] == '1':
        return EMPTY

    return -1


def parse_deletion_time(stream):
    # TODO whats up with this byte ?
    stream.read(1)

    local_deletion_time = read_int32(stream)
    marked_for_delete_at = read_int64(stream)

    return local_deletion_time, marked_for_delete_at
