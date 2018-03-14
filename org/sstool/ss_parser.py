import mmap
from contextlib import contextmanager


@contextmanager
def load_ss_data_file(path):
    with open(path, "r+") as f:
        try:
            map = mmap.mmap(f.fileno(), 0)
            yield map

        except Exception as e:
            print(e)

        finally:
            map.close()
