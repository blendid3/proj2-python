from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass

##  brandon code
# def sync(directory_address):
Metadata={}
StoreBlock={}
## class file:
class info_meta:
    def __init__(self,filename, version,hashlist):
        self.version = version
        self.hashlist=hashlist
        self.filename=filename

    def getfileinfomap(self,path):
        with open(path + "/" + "index.txt", "a") as fw:
            for i in metastorage:
                print(i)
                fw.write(str(i) + " " + str(metastorage[i].version) + " " + metastorage[i].hashlist + "\n")
    def clear_file(self,path):
        with open(path + "/" + "index.txt", "w+") as fw:
            fw.write("")
    def update_file(self, version,hashlist):
        self.version = version
        self.hashlist=hashlist
##
def getfilesize(filename):
    ##filename=
    with open(filename, "rb") as fr:
        fr.seek(0, 2)  # move to end of the file
        size = fr.tell()
        return fr.tell()

def filesplit(filename,readlimit):
    # filename = r"C:\Users\zhqbl\OneDrive\桌面\CSE224-master\hw_2\proj2-python\src\base_directory_client1\1mb-test_csv.csv"
    filesize = getfilesize(filename)
    # readlimit = 4096
    n_splits = math.ceil(filesize / readlimit)
    datalist=[]
    with open(filename, "rb") as fr:
        for i in range(n_splits):
            data = fr.read(readlimit)  # read
            datalist.append(data)
    return datalist

## input datalist
## output hashlist and StoreBlock
def creat_hashlist(datalist):
    hashlist=[]
    for data in datalist:
        sha256.update(data)
        StoreBlock[sha256.hexdigest()] = data
        hashlist.append(sha256.hexdigest())
    return hashlist
##

def update_metastorage(file,hashlist):
    # Check if dict contains any entry with key 'test'
    if file in Metadata:
        Metadata[file]
    else:
        file_info = info_meta(version=1,filename=file,hashlist=hashlist)
        Metadata[file]=file_info
##

##
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
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))
