import argparse
import xmlrpc.client
import os
import sys
import hashlib
from pathlib import Path


class Client:
    def __init__(self, args):
        self.basedir = args.basedir
        self.hostport = args.hostport
        self.blocksize = args.blocksize
        self.client = None
        self.error = False

        try:
            self.blocksize = int(self.blocksize)
            self.hostport = str(self.hostport)
            self.basedir = str(self.basedir)
            client = xmlrpc.client.ServerProxy(self.hostport)
            client.surfstore.ping()
            print("Ping() successful")
        except Exception as e:
            self.error = True
            print("Client __init__: " + str(e))

    # chunk: bytes
    def hash_chunk(self, chunk):
        sha256 = hashlib.sha256()
        sha256.update(chunk)
        return sha256.hexdigest()

    # path: Path object according to https://docs.python.org/zh-cn/3/library/pathlib.html
    def get_file_size(self, path):
        size = path.stat().st_size
        print("getfilesize: size: %s" % size)
        return size
    
    def sync(self):
        files = os.listdir(self.basedir)  # 得到文件夹下的所有文件名称
        for file in files:  # 遍历文件夹
            if not os.path.isdir(file):  # 判断是否是文件夹，不是文件夹才打开
                # 打开文件 the absolute path is path+file
                f = open(self.basedir + "/" + file, 'rb')
                buf = f.read()  # 读取所有内容
                # 打印出该文件的名字，以及hash内容
                print("{} ".format(os.path.basename(args.file)))

    # path: Path object according to https://docs.python.org/zh-cn/3/library/pathlib.html
    def split_file(self, path):
        # Open original file in read only mode
        # Do we need to consider symbolic links?
        if not path.is_file():
            print("No such file as: \"%s\"" % str(path))
            return

        filesize = get_file_size(path)
        filename = str(path)
        with open(filename, "rb") as fr:
            counter = 1
            original_filename = filename.split(".")
            readlimit = self.blocksize  # read 5kb at a time
            n_splits = filesize // self.blocksize
            print("splitfile: No of splits required: %s" % str(n_splits))
            for i in range(n_splits + 1):
                chunks_count = int(self.blocksize)//int(readlimit)
                data_chunk = fr.read(readlimit)  # read
                print(data_chunk)

                # Create split files
                print("chunks_count: %d" % chunks_count)
                with open(original_filename[0]+"_{id}.".format(id=str(counter))+original_filename[1], "ab") as fw:
                    fw.seek(0)
                    fw.truncate()  # truncate original if present
                    while data_chunk:
                        fw.write(data_chunk)
                        if chunks_count:
                            chunks_count -= 1
                            data_5kb = fr.read(readlimit)
                        else:
                            break
                counter += 1

def main():
    # the right format is: python client.py http://localhost:8080 basedir_address 1024(for example)
    # ./: current dir
    # Slash + input plus /
    # Tilda
    # Assume that the block size is not changing. Delete everything for different block size.
    parser = argparse.ArgumentParser(description="SurfStore client")
    parser.add_argument('hostport', help='host:port of the server')
    parser.add_argument('basedir', help='The base directory')
    parser.add_argument('blocksize', type=int, help='Block size')
    args = parser.parse_args()
    client = Client(args)
    if client.error:
        exit(1)

if __name__ == "__main__":
    main()
