import requests
import rpyc
import hashlib
import os
import sys
import glob

import time

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
        self.method = 1
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
            if line.startswith("method"):
                self.method = int(line.split(": ")[1])
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

    #返回最近的blockstore的index
    def find_nearest_server(self):
        min = 1000000
        wanted_server = None
        for index, (host, port) in enumerate(self.blockhosts):
            t1 = time.time()
            print(host)
            response = os.system("ping -c 1 " + host)
            t2 = time.time()
            if  t2 - t1 < min:
                min = t2 - t1
                wanted_server = index
        return wanted_server





    def upload(self, filepath):
        filepath = os.path.abspath(filepath)
        k = filepath.rfind("/")
        #k = filepath.rfind("\\")
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
        #find nearest server
        if self.method == 2:
            index = self.find_nearest_server()
        while True:
            try:
                print(self.method)
                if self.method == 1:
                    self.metadata.root.modify_file(filename,v,tuple(hashlist))
                if self.method == 2:
                    print("upload")
                    self.metadata.root.modify_file2(filename, v, tuple(hashlist), index)
                print("OK")
                break
            except rpyc.core.vinegar.GenericException as e:
                if e.error_type == 1:
                    self.eprint(e.error)
                    for hash in e.missing_blocks:
                        block = self.pathToDict[dicpath][hash]
                        if self.method == 1:
                            blockstore = self.blockstores[self.findServer(hash)]
                            blockstore.root.store_block(hash, block)
                        if self.method == 2:
                            blockstore = self.blockstores[index]
                            blockstore.root.store_block(hash,block)
                if e.error_type == 2:
                    self.eprint(e.error)
                    v = e.current_version + 1


    def findServer(self,h):
         return int(h, 16) % self.numBlockStores
    def delete(self, filename):
        v, hashlist = self.metadata.root.read_file(filename)
        v += 1
        while True:
            try:
                name,v2 = self.metadata.root.delete_file(filename, v)
                if v2 == 0:
                    print("Not found")
                    break
                print("OK")
                break
            except rpyc.core.vinegar.GenericException as e:
                self.eprint(e.error)
                v = e.current_version + 1




    def download(self, filename, location):
        dicpath = os.path.abspath(location)
        # read all files
        files = os.listdir(dicpath)
        size = 4096
        data = []
        self.pathToDict[dicpath] = {}
        for file in files:
            if os.path.isdir(file):
                continue
            #with open(dicpath + "\\" + file,"rb") as f:
            #todo change path to linux version
            with open(dicpath + "/" + file,"rb") as f:
                bytes = f.read(size)
                while bytes != b"":
                    data.append(bytes)
                    bytes = f.read(size)
            f.close()
            for block in data:
                hash = self.convert(block)
                self.pathToDict[dicpath][hash] = block
        #download
        v, hashlist = self.metadata.root.read_file(filename)
        if len(hashlist) == 0:
            print("Not Found")
            return
        #file = open(dicpath +"\\" + filename,'wb')
        if self.method == 2:
            server_index = self.metadata.root.exposed_get_nearest(filename)
        file = open(dicpath +"/" + filename,'wb')
        if self.method == 2:
            blockstore = self.blockstores[server_index]
        for hash in hashlist:
            if not dicpath in self.pathToDict:
                self.pathToDict[dicpath] = {}
            if not hash in self.pathToDict[dicpath]:
                if self.method == 1:
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

