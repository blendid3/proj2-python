from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass



##  brandon code
# def sync(directory_address):
def creat_hashlist(datalist):
    hashlist = []
    for data in datalist:
        sha256.update(data)
        hashlist.append(sha256.hexdigest())
    return hashlist
##local_map : {} filename-> file_info(class version and hashlist)
##meta_map : {}filename-> file_info(class version and hashlist)
##client directory path:

meta_map ={}
block_map={}

##
def get_metamap():
    #global meta_map
    return  meta_map
# A simple ping, returns true
def ping():
    """A simple ping method"""
    print("Ping()")
    return True

# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""
    print("GetBlock(" + h + ")")
    blockData=block_map[h]
    #blockData = bytes(4)
    return blockData

# Puts a block
def putblock(b):
    """Puts a block"""
    sha256 = hashlib.sha256()
    print("PutBlock()")
    hash=sha256.update(b)
    block_map[hash]=b
    return True

# Given a list of blocks, return the subset that are on this server
def hasblocks(blocklist):
    """Determines which blocks are on this server"""
    print("HasBlocks()")
    blocklist_subset=[]
    for block in blocklist:
        if  block in block_map.values():
            blocklist_subset.append(block)
    return blocklist_subset

def update_file_info(filename,version,hashlist):
    meta_map[filename]=[version,hashlist]

##

##
def update_block_map(hashlist,blocklist):
    if hashlist[0]=="0":
        return
    for i in range(len(hashlist)):
        block_map[hashlist[i]]=blocklist[i]

# Retrieves the server's FileInfoMap
# input hashlist , filename
# make Meta_map,
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
    
    result["file1.dat"] = file1info#metamap

    return result

# Update a file's fileinfo entry
#have something wrong
def updatefile(filename, version, blocklist):
    """Updates a file's fileinfo entry"""
    print("UpdateFile()")
    meta_map[filename] = [version, hashlist]
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
        server = threadedXMLRPCServer(('localhost', 8080), requestHandler=RequestHandler,allow_none=True)
        server.register_introspection_functions()
        server.register_function(ping,"surfstore.ping")
        server.register_function(getblock,"surfstore.getblock")
        server.register_function(putblock,"surfstore.putblock")
        server.register_function(hasblocks,"surfstore.hasblocks")
        server.register_function(getfileinfomap,"surfstore.getfileinfomap")

        server.register_instance(meta_map)
        server.register_instance(block_map)


        server.register_function(updatefile,"surfstore.updatefile")
        server.register_function(update_file_info, "surfstore.update_file_info")
        server.register_function(get_metamap, "surfstore.get_metamap")
        server.register_function(update_block_map, "surfstore.update_block_map")


        server.register_function(isLeader,"surfstore.isleader")
        server.register_function(crash,"surfstore.crash")
        server.register_function(restore,"surfstore.restore")
        server.register_function(isCrashed,"surfstore.iscrashed")

        ## brandon

        ##
        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()
    except Exception as e:
        print("Server: " + str(e))
