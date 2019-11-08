import argparse
import xmlrpc.client
import os
import hashlib
from pathlib import Path
import shutil
import copy
import time


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
                while index_txt_path.exists():
                    pass
                index_txt_path.touch(mode=0o644, exist_ok=False)
            else:
                raise FileNotFoundError(
                    "Unable to create the required index.txt file!")
        lines = [line.split()
                 for line in index_txt_path.read_text().splitlines()]
        file_info_map = {}
        for line in lines:
            if len(line) < 3:
                raise ValueError("Index.txt is not correctly formatted!")
            file_info_map[line[0]] = [int(line[1]), line[2:]]
        return file_info_map

    def update_file_info_map(self, file_info_map):
        # Get all file names in this directory if it is not a directory and is not index.txt
        files = [x for x in self.basedir.iterdir() if x.is_file()
                 and x.name != "index.txt"]
        names = frozenset([x.name for x in files])
        previous_names = frozenset(file_info_map.keys())
        new_files = names - previous_names
        removed_files = previous_names - names
        others = names & previous_names

        for name in removed_files:
            if file_info_map[name][1] != ["0"]:
                file_info_map[name][1] = ["0"]
                file_info_map[name][0] += 1

        for name in new_files:  # Iterate folder
            # Get the chunks and the SHA-256 hash of each trunk
            chunks, hashes = self.split_and_hash_file(self.basedir / name)
            file_info_map[name] = [1, hashes]

        for name in others:
            # Get the chunks and the SHA-256 hash of each trunk
            chunks, hashes = self.split_and_hash_file(self.basedir / name)
            if hashes != file_info_map[name][1]:
                file_info_map[name][0] += 1
                file_info_map[name][1] = hashes

    def write_file_info_map(self, file_info_map):
        Path(self.basedir / "index.txt").write_text("\n".join([" ".join([filename, str(
            info[0]), " ".join(info[1])]) for filename, info in file_info_map.items()]))

    ## the function is to create the local_map and compare the file in directory with local map
    ## if the index.txt doesn't exist, then create new index.txt, and set all version is 1
    ## if the index.txt exist:
    ## 1. if the file modified, the version plus 1 and the hashlist change
    ## 2. if the file deleted , the version plus 1 and the hashlilst become \0
    ## 3. if the file created, the new map created and the version become 1
    ##
    #configure:path and read_limit

    def upload_server(client, path, local_map, meta_map):
        for file in local_map:
            if file != "index.txt" and file in meta_map:
                if local_map[file].version > meta_map[file].version:  # can upload
                    if local_map[file].hashlist != meta_map[file].hashlist:
                        file_path = path+"/"+file
                        blocklist = creat_datelist(file_path, read_limit)
                        # update the meta_map of server
                        client.update_file_info(local_map[file])
                        client.update_block_map(
                            local_map[file].hashlist, blocklist)
                        meta_map[file] = local_map[file]
            else:
                if file != "index.txt":
                    file_path = path + "/" + file
                    blocklist = creat_datelist(file_path, read_limit)
                    print(local_map[file].filename)
                    print(local_map[file].hashlist)
                    # update the meta_map of server
                    client.update_file_info(
                        local_map[file].filename, local_map[file].version, local_map[file].hashlist)
                    client.update_block_map(
                        local_map[file].hashlist, blocklist)
                    meta_map[file] = local_map[file]

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

        local_file_info_map = copy.deepcopy(file_info_map)
        server_file_info_map = self.client.surfstore.getfileinfomap()

        new_file_info_map = copy.deepcopy(local_file_info_map)
        self.update_file_info_map(new_file_info_map)
        self.write_file_info_map(new_file_info_map)


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
    client = Client(parser.parse_args())
    if client.error:
        return
    client.sync()


if __name__ == "__main__":
    main()
