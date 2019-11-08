import argparse
import xmlrpc.client
import os
import hashlib
from pathlib import Path
import shutil


class Client:
    def __init__(self, args):
        self.client = None
        self.error = False

        try:
            self.blocksize = int(args.blocksize)
            self.basedir = Path(str(args.basedir)).resolve(
                strict=False)  # Requires Python 3.6
            self.hostport = str(args.hostport)
            self.client = xmlrpc.client.ServerProxy(self.hostport)
            assert self.blocksize > 0

        except Exception as e:
            self.error = True
            print("Client __init__(): " + str(e))

    # chunk: bytes
    def hash_chunk(self, chunk):
        sha256 = hashlib.sha256()
        sha256.update(chunk)
        return sha256.hexdigest()

    # path: Path object according to https://docs.python.org/zh-cn/3/library/pathlib.html
    def get_file_size(self, path):
        if not path.is_file():
            print("No such file as: \"%s\"" % str(path))
            return

        path = path.resolve(strict=False)  # Requires Python 3.6
        size = path.stat().st_size
        print("getfilesize: size: %s" % size)
        return size

    # path: Path object according to https://docs.python.org/zh-cn/3/library/pathlib.html
    def split_and_hash_file(self, path):
        if not path.is_file():
            print("No such file as: \"%s\"" % str(path))
            return

        path = path.resolve(strict=False)  # Requires Python 3.6
        filesize = self.get_file_size(path)
        bytes = path.read_bytes()
        assert len(bytes) == filesize

        chunks = [bytes[chunk_count * self.blocksize: (chunk_count + 1) * self.blocksize]
                  for chunk_count in range(-(-len(bytes) // self.blocksize))]
        hashes = [self.hash_chunk(chunk) for chunk in chunks]
        return chunks, hashes

    def load_file_info_map(self):
        index_txt_path = Path(self.basedir / "index.txt")
        another_kind_of_file = False
        if index_txt_path.is_symlink():
            another_kind_of_file = True
        elif not index_txt_path.exists():
            index_txt_path.touch(mode=0o644)
        elif not index_txt_path.is_file():  # it is not a regular file
            another_kind_of_file = True
        if another_kind_of_file:
            if input("Do you want to remove index.txt in order to use the SurfStore client? [Y/N]").lower() == "y":
                try:
                    index_txt_path.unlink()  # remove file or symbolic link
                except Exception:  # Might be a path
                    try:
                        shutil.rmtree(str(index_txt_path))  # remove path
                    except Exception as e:
                        raise FileNotFoundError(
                            "Unable to create the required index.txt file!")
                index_txt_path.touch(mode=0o644, exist_ok=False)
            else:
                raise FileNotFoundError(
                    "Unable to create the required index.txt file!")
        return index_txt_path.read_text().splitlines()

    def sync(self):
        try:
            assert self.basedir.is_dir()
            assert os.access(str(self.basedir), os.R_OK)
            assert os.access(str(self.basedir), os.W_OK)
            assert os.access(str(self.basedir), os.X_OK)

            self.client.surfstore.ping()
            print("Ping() successful")

        except Exception as e:
            print("Client sync() init: " + str(e))
            return

        file_info_map = None
        try:
            file_info_map = self.load_file_info_map()  # Read index.txt to memory
        except Exception as e:
            print("Client sync() file info map loading: " + str(e))
            return

        # Get all file names in this directory
        files = [x for x in self.basedir.iterdir()]
        for file in files:  # Iterate folder
            # Open the file only if it is not a directory and is not index.txt
            if file.name != "index.txt" and file.is_file():
                # Get the chunks and the SHA-256 hash of each trunk
                chunks, hashes = self.split_and_hash_file(file)


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
    client.sync()


if __name__ == "__main__":
    main()
