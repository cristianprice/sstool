from os import listdir
from os.path import isfile, join


def parse_data_dir(path):
    '''
    Lists all files in data dir.
    :param path:
    :return:
    '''
    for f in listdir(path):
        full_path = join(path, f)
        if isfile(full_path):
            yield full_path
