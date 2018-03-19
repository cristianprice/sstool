from org.sstool.ss_index_parser import parse_ss_index_file

if __name__ == '__main__':
    path = '/home/cristian/ss_test/accounts-f3304f5ae9a6320c88eb8cde3dbb3422/mc-115497-big-Index.db'
    path_local = '/opt/cassandra/data/data/test/cyclist-53fde6e0276c11e891ccd132782c8a9c/mc-1-big-Index.db'

    path_ul = '/home/cristian/tmp/ss_tables/mc-254981-big-Index.db'

    key_length = 1
    for val in parse_ss_index_file(path_ul):
        print(val)
        for i in val.get_promoted_indexes():
            print(i)

    print('Done.')
