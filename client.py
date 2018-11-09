import rpyc
import hashlib
import os
import sys

"""
A client is a program that interacts with SurfStore. It is used to create,
modify, read, and delete files.  Your client will call the various file
modification/creation/deletion RPC calls.  We will be testing your service with
our own client, and your client with instrumented versions of our service.
"""
class ErrorResponse(Exception):
    def __init__(self, message):
        super(ErrorResponse, self).__init__(message)
        self.error = message

    def missing_blocks(self, hashlist):
        self.error_type = 1
        self.missing_blocks = hashlist

    def wrong_version_error(self, version):
        self.error_type = 2
        self.current_version = version

    def file_not_found(self):
        self.error_type = 3


class SurfStoreClient():


    """
    Initialize the client and set up connections to the block stores and
    metadata store using the config file
    """
    def __init__(self, config):

        self.blockhosts = []
        self.blockstores = []
        self.pathToDict ={}

        with open(config) as f:
            lines = f.readlines()
        for line in lines:
            if line.startswith("B"):
                self.numBlockStores = int(line.split(": ")[1])
            if line.startswith("block"):
                host = line.split(": ")[1].split(":")[0]
                port = line.split(": ")[1].split(":")[1]
                port = port.split("\n")[0]
                self.blockhosts.append((host, port))
            if line.startswith("metadata"):
                host = line.split(": ")[1].split(":")[0]
                port = line.split(": ")[1].split(":")[1]
                port = port.split("\n")[0]
                self.metadata = rpyc.connect(host, port)

        for (host, port) in self.blockhosts:
            blockstore = rpyc.connect(host, port)
            self.blockstores.append(blockstore)

    """
    upload(filepath) : Reads the local file, creates a set of 
    hashed blocks and uploads them onto the MetadataStore 
    (and potentially the BlockStore if they were not already present there).
    """
    def convert(self,b):
        return hashlib.sha256(b).hexdigest()


    def upload(self, filepath):
        filepath = os.path.abspath(filepath)
        k = filepath.rfind("\\")
        filename = filepath[k + 1:]
        dicpath = filepath[:k]
        v,hashlist = self.metadata.root.read_file(filename)
        #get hashlist
        hashlist = []
        size = 4096
        data = []
        try:
            with open(filepath,"rb") as f:
                bytes = f.read(size)
                while bytes != b"":
                    data.append(bytes)
                    bytes = f.read(size)
            f.close()
            #todo 不用throw exception,return a pair where the version is the first element, and the hash list is empty.
        except:
            print("Not Found")
            return
        for block in data:
            hash = self.convert(block)
            hashlist.append(hash)
            if dicpath not in self.pathToDict.keys():
                self.pathToDict[dicpath] = {}
            self.pathToDict[dicpath][hash] = block
        v += 1
        while True:
            try:
                self.metadata.root.modify_file(filename,v,tuple(hashlist))
                print("OK")
                print(filename)
                break
            except rpyc.core.vinegar.GenericException as e:
                if e.error_type == 1:
                    self.eprint(e.error)
                    for hash in e.missing_blocks:
                        blockstore = self.blockstores[self.findServer(hash)]
                        block = self.pathToDict[dicpath][hash]
                        blockstore.root.store_block(hash,block)
                if e.error_type == 2:
                    self.eprint(e.error)
                    v = e.current_version + 1

    def findServer(self,h):
         return int(h, 16) % self.numBlockStores
    def delete(self, filename):
        v, hashlist = self.metadata.root.read_file(filename)
        v += 1
        print(v)
        while True:
            try:
                self.metadata.root.delete_file(filename,v)
                print("OK")
                break
            except rpyc.core.vinegar.GenericException as e:
                self.eprint(e.error)
                v = e.current_version + 1




    def download(self, filename, location):
        dicpath = os.path.abspath(location)
        v, hashlist = self.metadata.root.read_file(filename)
        if len(hashlist) == 0:
            print("Not Found")
            return
        file = open(dicpath + filename,'wb')
        for hash in hashlist:
            if not dicpath in self.pathToDict:
                self.pathToDict[dicpath] = {}
            if not hash in self.pathToDict[dicpath]:
                blockstore = self.blockstores[self.findServer(hash)]
                block = blockstore.root.get_block(hash)
                self.pathToDict[dicpath][hash] = block
                file.write(block)
            else:
                file.write(self.pathToDict[dicpath][hash])
        print("OK")
        file.close()

    """
     Use eprint to print debug messages to stderr
     E.g -
     self.eprint("This is a debug message")
    """
    def eprint(*args, **kwargs):
        print(*args, file=sys.stderr, **kwargs)



if __name__ == '__main__':
    client = SurfStoreClient(sys.argv[1])
    operation = sys.argv[2]
    if operation == 'upload':
        client.upload(sys.argv[3])
    elif operation == 'download':
        client.download(sys.argv[3], sys.argv[4])
    elif operation == 'delete':
        client.delete(sys.argv[3])
    else:
        print("Invalid operation")

