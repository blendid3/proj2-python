from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

##  brandon code
# def sync(directory_address):
##local_map : {} filename-> file_info(class version and hashlist)
##meta_map : {}filename-> file_info(class version and hashlist)
##client directory path:
local_map={}
meta_map ={}
block_map={}# hashlist-> block
path=""
def synchronize(path,local_map):
    upload_server(path,local_map)
    download_client(path,local_map)
    update_indexfile(path,local_map)

## delete the server file
def delete_file(file):
    if file in meta_map.key():
        meta_map(file).hashlist = "\0"
        meta_map(file).version += 1
    else:
        print("file is not exist")

class file_info:
    def __init__(self,filename, version,hashlist):
        self.version = version
        self.hashlist=hashlist
        self.filename=filename
def upload_server(path,local_map):
    for file in local_map.key():
        if file != "index.txt" and file in Meta_map.key():
            if local_map(file).version == meta_map(file).version:
                if local_map(file).hashlist !=meta_map(file).hashlist:
                    upload(file,path)
                    meta_map(file).version+=1
                    local_map(file).version+=1

        else:
            upload(file,path)
            new_file=info_meta(version=1, filename=file, hashlist=local_map(file).hashlist)
            meta_map[file]=new_file

def download_client(path,local_map):
    for file in meta_map.key():
        if file in local_map.key():
            if local_map(file).version < meta_map(file).version:
                if meta_map(file).hashlist !="\0":
                    download_file(file,path)
                else:
                    delete_client_file(file)
        else:
            download_file(file)

def update_indexfile(path):
    ## update index file
    with open(path + "/" + "index.txt", "w+") as fw:
        for file in meta_map.key():
            fw.write(file+ " "+ str(meta_map[file].version) +" "+ meta_map[file].hashlist+"\n")


def upload(file,path):
    with open(path + "/" + file, "rb") as fr:
        fr.read()
        fr.seek(0, 2)  # move to end of the file
        filesize = fr.tell()
        read_limit=4096
        n_splits = math.ceil(filesize / readlimit)
        datalist = []
        for i in range(n_splits):
            data = fr.read(readlimit)  # read
            datalist.append(data)
    hash_list=creat_hashlist(datalist)
    for i in range(len(hash_list)):
        block_map[hash_list[i]]=datalist[i]## update the block_map
    meta_map[file].hashlist=hash_list##updat the meta_map



def creat_hashlist(datalist):
    hashlist = []
    for data in datalist:
        sha256.update(data)
        StoreBlock[sha256.hexdigest()] = data
        hashlist.append(sha256.hexdigest())
    return hashlist

def download_file(file,path):
    with open(path + "/" + file, "w+") as fw:
        fw.write("")
    with open(path + "/" + file, "ab") as fw:
        hash_list=meta_map(file).hashlist
        for hash in hash_list:
            fw.write(block_map[hash])
## delete command
def delete_client_file(file,path):
    if os.path.exists(path+"/"+file):
        os.remove(path+"/"+file)
    else:
        print("Can not delete the file as it doesn't exists")
# A simple ping, returns true
def ping():
    """A simple ping method"""
    print("Ping()")
    return True

# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""
    print("GetBlock(" + h + ")")

    blockData = bytes(4)
    return blockData

# Puts a block
def putblock(b):
    """Puts a block"""
    print("PutBlock()")

    return True

# Given a list of blocks, return the subset that are on this server
def hasblocks(blocklist):
    """Determines which blocks are on this server"""
    print("HasBlocks()")

    return blocklist

# Retrieves the server's FileInfoMap
def getfileinfomap():
    """Gets the fileinfo map"""
    print("GetFileInfoMap()")

    result = {}

    # file1.dat
    file1info = []
    file1info.append(3) // version

    file1blocks = []
    file1blocks.append("h1")
    file1blocks.append("h2")
    file1blocks.append("h3")

    file1info.append(file1blocks)
    
    result["file1.dat"] = file1info

    return result

# Update a file's fileinfo entry
def updatefile(filename, version, blocklist):
    """Updates a file's fileinfo entry"""
    print("UpdateFile()")

    return True

# PROJECT 3 APIs below

# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    print("IsLeader()")
    return True

# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    print("Crash()")
    return True

# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    print("Restore()")
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    return True

if __name__ == "__main__":
    try:
        print("Attempting to start XML-RPC Server...")
        server = threadedXMLRPCServer(('localhost', 8080), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping,"surfstore.ping")
        server.register_function(getblock,"surfstore.getblock")
        server.register_function(putblock,"surfstore.putblock")
        server.register_function(hasblocks,"surfstore.hasblocks")
        server.register_function(getfileinfomap,"surfstore.getfileinfomap")
        server.register_function(updatefile,"surfstore.updatefile")

        server.register_function(isLeader,"surfstore.isleader")
        server.register_function(crash,"surfstore.crash")
        server.register_function(restore,"surfstore.restore")
        server.register_function(isCrashed,"surfstore.iscrashed")

        ## brandon
        server.register_function(synchronize,"surfstore.synchronize")
        server.register_function(delete_file, "surfstore.delete_file")
        ##
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))
