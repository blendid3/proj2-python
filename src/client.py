import os
import math
import hashlib
import argparse
import xmlrpc.client
sha256 = hashlib.sha256()
class file_info:
    def __init__(self, filename, version, hashlist):
        self.version = version
        self.hashlist = hashlist
        self.filename = filename
def read_index(path):
    local_map = {}
    with open(path + "/" + "index.txt", "r") as index_file:
        for line in index_file:
            info = line.split()
            hashlist=[]
            hashlist.append(info[2])
            new_info = file_info(info[0], int(info[1]),hashlist)
            local_map[info[0]] = new_info
    return local_map


def creat_datelist(file_path, read_limit):
    with open(file_path, "rb") as fr:
        fr.read()
        fr.seek(0, 2)  # move to end of the file
        filesize = fr.tell()

    with open(file_path, "rb") as fr:
        n_splits = math.ceil(filesize / read_limit)
        datalist = []
        for i in range(n_splits):
            data = fr.read(read_limit)  # read
            datalist.append(data)
    return datalist

def creat_hashlist(datalist):
    hashlist = []
    for data in datalist:
        sha256.update(data)
        hashlist.append(sha256.hexdigest())
    return hashlist

def creat_local_map(path,read_limit):
    files = os.listdir(path)  # 得到文件夹下的所有文件名称
    local_map={}
    for file in files:  # 遍历文件夹
        if not file =="index.txt" :
            file_path=path+"/"+file
            datalist=creat_datelist(file_path, read_limit)
            hash_list = creat_hashlist(datalist)
            new_info=file_info(file,1,hash_list)
            local_map[file]=new_info
    return local_map

def update_local_map(file_info,local_map):
    if file_info.filename in local_map:
        local_map[file_info.filename]=file_info
    return local_map




def creat_map(path,read_limit):
    files = os.listdir(path)
    if not "index.txt" in files:
        local_map = creat_local_map(path, read_limit)
    else:
        local_map = read_index(path)
        for file in files:
            if not file == "index.txt":
                file_path = path + "/" + file
                datalist = creat_datelist(file_path, read_limit)
                hashlist = creat_hashlist(datalist)

                if not file in local_map:
                    new_info = file_info(file, version=1, hashlist=hashlist)
                    local_map[new_info.filename]=new_info
                else:
                    if not local_map[file].hashlist == hashlist:

                        local_map[file].hashlist = hashlist
                        local_map[file].version += 1

        for file in local_map:
            if not file in files:
                local_map[file].hashlist = "\0"
                local_map[file].version += 1
    return local_map
## the function is to create the local_map and compare the file in directory with local map
## if the index.txt doesn't exist, then create new index.txt, and set all version is 1
## if the index.txt exist:
## 1. if the file modified, the version plus 1 and the hashlist change
## 2. if the file deleted , the version plus 1 and the hashlilst become \0
## 3. if the file created, the new map created and the version become 1
##
#configure:path and read_limit


def upload_server(client,path, local_map,meta_map):
    for file in local_map:
        if file != "index.txt" and file in meta_map:
            if local_map[file].version > meta_map[file].version:## can upload
                if local_map[file].hashlist != meta_map[file].hashlist:
                    file_path=path+"/"+file
                    blocklist=creat_datelist(file_path, read_limit)
                    client.update_file_info(local_map[file])## update the meta_map of server
                    client.update_block_map(local_map[file].hashlist,blocklist)
                    meta_map[file]=local_map[file]
        else:
            if file != "index.txt":
                file_path = path + "/" + file
                blocklist = creat_datelist(file_path, read_limit)
                print(local_map[file].filename)
                print(local_map[file].hashlist)
                client.update_file_info(local_map[file].filename,local_map[file].version,local_map[file].hashlist)  ## update the meta_map of server
                client.update_block_map(local_map[file].hashlist, blocklist)
                meta_map[file] = local_map[file]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SurfStore client")
    parser.add_argument('hostport', help='host:port of the server')
    parser.add_argument('client_dir', help='The base directory')
    parser.add_argument('blocksize', type=int, help='Block size')
    args = parser.parse_args()
    path = args.client_dir
    read_limit = args.blocksize
    local_map = creat_map(path, read_limit)## finish creating local map
    client = xmlrpc.client.ServerProxy(args.hostport)
    meta_map = client.surfstore.get_metamap()# get the meta_map from server
    upload_server(client, path, local_map, meta_map)## update the meta_map and block map of the server
    meta_map = client.surfstore.get_metamap()
    for key in meta_map:
        print(key)
        print(local_map[key].version)
        print(local_map[key].hashlist)


## the right format is: python client.py http://localhost:8080 basedir_address 1024(for example)




    #path=r"C:\Users\zhqbl\OneDrive\桌面\CSE224-master\hw_2\proj2-python\src\base_directory_client1"