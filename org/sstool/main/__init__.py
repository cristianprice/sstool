from org.sstool.utils import parse_data_dir

if __name__ == '__main__':

    query = 'INSERT INTO test.cyclist ( name, nationality )'

    for f in parse_data_dir('C:\\apache-cassandra-3.11.2\\data\\data\\test\\cyclist-a0a7aaf0270611e89b8bcf1f8c3b14e4'):
        print(f)
