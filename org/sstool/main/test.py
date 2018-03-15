from struct import unpack

b = bin(131)[2:].zfill(8)

path = '/home/cristian/ss_test/accounts-f3304f5ae9a6320c88eb8cde3dbb3422/mc-115497-big-Index.db'
with open(path, mode='rb') as f:
    while True:
        b = f.read(1)
        if b != '':
            print(unpack('b', b))
