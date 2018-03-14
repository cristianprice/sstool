from org.sstool.ss_parser import load_ss_data_file
from org.sstool.utils import read_short
from org.sstool.utils import read_bytes
from org.sstool.utils import read_varint
from uuid import UUID

if __name__ == '__main__':
    path = '/home/cristian/ss_test/accounts-f3304f5ae9a6320c88eb8cde3dbb3422/mc-115497-big-Index.db'
    path_local = '/opt/cassandra/data/data/test/cyclist-53fde6e0276c11e891ccd132782c8a9c/mc-1-big-Index.db'

    key_length = 1
    with load_ss_data_file(path) as mf:
        while True:
            key_length = read_short(mf)
            key = UUID(bytes=read_bytes(mf, key_length))
            position = read_varint(mf)
            promoted_idx_len = read_varint(mf)

            if promoted_idx_len != 0:
                print(promoted_idx_len)

            promoted_index = read_bytes(mf, promoted_idx_len)
            print(key_length, key, position, promoted_idx_len)

    print('Done.')
