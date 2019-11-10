import argparse
import hashlib
import os
import shutil
import time
import xmlrpc.client
from pathlib import Path


class Client:
    def __init__(self, args):
        self.deleted_hashes = ["0"]
        self.client = None
        self.error = False

        try:
            self.blocksize = int(args.blocksize)
            self.basedir = Path(str(args.basedir)).resolve(
                strict=False)  # Requires Python 3.6
            self.client = xmlrpc.client.ServerProxy(
                "http://" + str(args.hostport))
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
    def get_blocks(self, path):
        if not path.is_file():
            print("No such file as: \"%s\"" % str(path))
            return []

        path = path.resolve(strict=False)  # Requires Python 3.6
        filesize = path.stat().st_size
        bytes = path.read_bytes()
        assert len(bytes) == filesize
        return [bytes[chunk_count * self.blocksize: (chunk_count + 1) * self.blocksize] for chunk_count in range(-(-len(bytes) // self.blocksize))]

    # path: Path object according to https://docs.python.org/zh-cn/3/library/pathlib.html
    def split_and_hash_file(self, path):
        chunks = self.get_blocks(path)
        hashes = [self.hash_chunk(chunk) for chunk in chunks]
        return chunks, hashes

    def write_file_info_map(self, file_info_map):
        return Path(self.basedir / "index.txt").write_text("\n".join([" ".join([filename, str(
            info[0]), " ".join(info[1])]) for filename, info in file_info_map.items()]))

    # path: Path object according to https://docs.python.org/zh-cn/3/library/pathlib.html
    def write_file(self, path, blocks):
        return path.write_bytes(b''.join(blocks))

    # path: Path object according to https://docs.python.org/zh-cn/3/library/pathlib.html
    def delete_file(self, path):
        try:
            path.unlink()  # remove file or symbolic link
        except Exception:  # Might be a path
            try:
                shutil.rmtree(str(path))  # remove path
            except Exception as e:
                raise FileNotFoundError(
                    "Unable to create the required index.txt file!")

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
                self.delete_file(index_txt_path)
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

    # the function is to create the local_map and compare the file in directory with local map
    # if the index.txt doesn't exist, then create new index.txt, and set all version is 1
    # if the index.txt exist:
    # 1. if the file modified, the version plus 1 and the hashlist change
    # 2. if the file deleted , the version plus 1 and the hashlist become 0
    # 3. if the file created, the new map created and the version become 1
    ##
    #configure:path and read_limit

    def update_and_upload_file_info_map(self, file_info_map):
        unsuccessful = False

        # Get all file names in this directory if it is not a directory and is not index.txt
        files = [x for x in self.basedir.iterdir() if x.is_file()
                 and x.name != "index.txt"]
        names = frozenset([x.name for x in files])
        previous_names = frozenset(file_info_map.keys())
        new_files = names - previous_names
        removed_files = previous_names - names
        others = names & previous_names

        for name in removed_files:
            if file_info_map[name][1] != self.deleted_hashes:
                file_info_map[name][1] = self.deleted_hashes
                file_info_map[name][0] += 1
                if not self.client.surfstore.updatefile(name, file_info_map[name][0], self.deleted_hashes):
                    unsuccessful = True

        for name in new_files:  # Iterate folder
            # Get the chunks and the SHA-256 hash of each trunk
            chunks, hashes = self.split_and_hash_file(self.basedir / name)
            file_info_map[name] = [1, hashes]
            blocks_exists_on_server = self.client.surfstore.hasblocks(hashes)
            for block in [chunks[i] for i in range(len(chunks)) if hashes[i] not in blocks_exists_on_server]:
                self.client.surfstore.putblock(block)
            if not self.client.surfstore.updatefile(name, 1, hashes):
                unsuccessful = True

        for name in others:
            # Get the chunks and the SHA-256 hash of each trunk
            chunks, hashes = self.split_and_hash_file(self.basedir / name)
            if hashes != file_info_map[name][1]:
                file_info_map[name][0] += 1
                file_info_map[name][1] = hashes
                blocks_exists_on_server = self.client.surfstore.hasblocks(
                    hashes)
                for block in [chunks[i] for i in range(len(chunks)) if hashes[i] not in blocks_exists_on_server]:
                    self.client.surfstore.putblock(block)
                if not self.client.surfstore.updatefile(name, file_info_map[name][0], hashes):
                    unsuccessful = True

        return not unsuccessful

    def download_from_server(self, file_info_map):
        server_file_info_map = self.client.surfstore.getfileinfomap()
        local_names = frozenset(file_info_map.keys())
        server_names = frozenset(server_file_info_map.keys())
        new_server_files = server_names - local_names
        common_files = server_names & local_names

        for name in new_server_files:
            path = Path(self.basedir / name)
            while server_file_info_map[name][1] != self.deleted_hashes and self.write_file(path, [self.client.surfstore.getblock(hash).data for hash in server_file_info_map[name][1]]) == 0:
                pass
            file_info_map[name] = server_file_info_map[name]

        for name in common_files:
            path = Path(self.basedir / name)
            if server_file_info_map[name][0] > file_info_map[name][0]:
                if server_file_info_map[name][1] != self.deleted_hashes:
                    blocks = self.get_blocks(path)
                    for i in range(min(len(server_file_info_map[name][1]), len(file_info_map[name][1]))):
                        if server_file_info_map[name][1][i] != file_info_map[name][1][i]:
                            blocks[i] = self.client.surfstore.getblock(
                                server_file_info_map[name][1][i]).data
                    if len(server_file_info_map[name][1]) > len(file_info_map[name][1]):
                        blocks.extend([self.client.surfstore.getblock(
                            hash).data for hash in file_info_map[name][1][len(file_info_map[name][1]):]])
                    self.write_file(path, blocks)
                elif path.exists():
                    self.delete_file(path)
                file_info_map[name] = server_file_info_map[name]

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

        while True:
            self.download_from_server(file_info_map)

            # Now if a subset of the files in your base directory are newer
            # than the remote, you need to call updatefile(). It is possible
            # updatefile() fails because someone else beat you to the cloud.
            # You'll download the newer version by calling getfileinfomap.
            # But don't then go back and check teh files you already checked
            # before just keep looping through your local updates then quit.
            if self.update_and_upload_file_info_map(file_info_map):
                break
        self.write_file_info_map(file_info_map)


def main():
    # the right format is: python client.py localhost:8080 basedir_address 1024(for example)
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
