from org.sstool.utils import read_short
from org.sstool.utils import read_int32
from org.sstool.utils import read_int64
from org.sstool.utils import read_bytes
from org.sstool.utils import read_varint
from io import BytesIO
from org.sstool.utils import load_ss_file


def parse_ss_index_file(path):
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
                promoted_index = read_bytes(mf, promoted_idx_len)

            yield SSRowIndexEntry(key, position, promoted_index)


class ClusteringPrefix(object):

    def __init__(self, kind):
        self.kind = kind


class SSIndexInfo(object):
    # Bound kinds
    EXCL_END_BOUND = 0
    INCL_START_BOUND = 1
    EXCL_END_INCL_START_BOUNDARY = 2
    STATIC_CLUSTERING = 3
    CLUSTERING = 4
    INCL_END_EXCL_START_BOUNDARY = 5
    INCL_END_BOUND = 6
    EXCL_START_BOUND = 7

    def __init__(self, pi_bytes):
        self.first_name = self.__parse_cluster_prefix(pi_bytes)
        self.last_name = self.__parse_cluster_prefix(pi_bytes)

    def __parse_cluster_prefix(self, pi_bytes):
        kind = read_bytes(1)

        if kind == self.CLUSTERING:
            return self.__deserializeClustering(pi_bytes)

        return self.__deserializeBoundOrBoundry()

    def __deserializeClustering(self, pi_bytes):
        pass

    def __deserializeBoundOrBoundry(self, pi_bytes):
        pass


class SSIndexEntry(object):

    def __init__(self, header_len, deletion_time, columns_index_count, pi_bytes):
        # Read header length
        self.header_len = header_len
        self.deletion_time = deletion_time
        self.columns_index_count = columns_index_count
        self.pi_bytes = pi_bytes

    def __parse_index_info(self, mf):
        yield SSIndexInfo()

    def get_index_infos(self):
        for c in range(self.columns_index_count):
            yield self.__parse_index_info(self.pi_bytes)

    def __str__(self):
        return "SSIndexEntry([{0},{1},{2}])".format(self.header_len, self.deletion_time, self.columns_index_count)


class SSRowIndexEntry(object):
    MAX_BE32 = 0x7FFFFFFF
    MIN_BE64 = 0x8000000000000000

    def __init__(self, key, position, promoted_index):
        self.key = key
        self.position = position
        self.promoted_index = promoted_index

    def __str__(self):
        return "SSRowIndexEntry([{0},{1}])".format(self.key, self.position)

    def get_promoted_indexes(self):
        if self.promoted_index != b'':
            pi_bytes = BytesIO(self.promoted_index)

            # Read header length
            header_len = read_varint(pi_bytes)
            deletion_time = self.__parse_deletion_time(pi_bytes)
            columns_index_count = read_varint(pi_bytes)

            for c in range(columns_index_count):
                yield SSIndexEntry(header_len, deletion_time, columns_index_count, pi_bytes)

        return None

    def __parse_deletion_time(self, stream):
        local_deletion_time = read_int32(stream)
        marked_for_delete_at = read_int64(stream)

        return local_deletion_time, marked_for_delete_at
