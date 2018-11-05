import rpyc
import sys


'''
A sample ErrorResponse class. Use this to respond to client requests when the request has any of the following issues - 
1. The file being modified has missing blocks in the block store.
2. The file being read/deleted does not exist.
3. The request for modifying/deleting a file has the wrong file version.

You can use this class as it is or come up with your own implementation.
'''
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



'''
The MetadataStore RPC server class.

The MetadataStore process maintains the mapping of filenames to hashlists. All
metadata is stored in memory, and no database systems or files will be used to
maintain the data.
'''



class MetadataStore(rpyc.Service):


    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """
    def __init__(self, config):
        self.fNamesToHList = {}
        self.fNamesToV = {}
        self.tombstone = []
        self.hosts = []
        self.blockstores = []

        with open(config) as f:
            lines = f.readlines()
        for line in lines:
            if line.startswith("B"):
                self.numBlockStores = int(line.split(": ")[1])
            if line.startswith("block"):
                host = line.split(": ")[1].split(":")[0]
                port = line.split(": ")[1].split(":")[1]
                port = port.split("\n")[0]
                self.hosts.append((host, port))
            if line.startswith("metadata"):
                self.host = line.split(": ")[1].split(":")[0]
                print(self.host)
        for (host,port) in self.hosts:
            blockstore = rpyc.connect(host,port)
            self.blockstores.append(blockstore)


    def findServer(self,h):
         return int(h, 16) % self.numBlockStores



    def exposed_modify_file(self, filename, version, hashlist):
            if not filename in self.fNamesToV:
                response = ErrorResponse("missingBlocks")
                response.missing_blocks(tuple(hashlist))
                print(len(hashlist))
                raise response
            if not self.fNamesToV[filename] + 1 == version:
                response = ErrorResponse("Error:Requires version >=" + str(self.fNamesToV[filename] + 1))
                response.wrong_version_error(self.fNamesToV[filename])
                raise response
            missingBlocks = []
            for hash in hashlist:
                i = self.findServer(hash)
                c = self.blockstores[i]
                if not c.root.has_block(hash):
                    missingBlocks.append(hash)

            if not len(missingBlocks) == 0:
                print(len(missingBlocks))
                response = ErrorResponse("missingBlocks")
                response.missing_blocks(tuple(missingBlocks))
                raise response

            self.fNamesToHList[filename] = hashlist
            self.fNamesToV[filename] += 1
            return "OK"


    '''
        DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
        version number v must be one bigger than the most up-date-date version.

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
    '''
    def exposed_delete_file(self, filename, version):
        if not filename in self.fNamesToV:
            return "OK"
        if not self.fNamesToV[filename] + 1 == filename:
            response = ErrorResponse("Error:Requires version >=" + str(self.fNamesToV[filename] + 1))
            response.wrong_version_error(self.fNamesToV[filename])
            raise response
        self.tombstone.append(filename)
        self.fNamesToV[filename] += 1
        return "OK"



    def exposed_read_file(self, filename):
        if filename not in self.fNamesToV:
            self.fNamesToV[filename] = 0
            return (0, [])
        if filename in self.tombstone:
            return (self.fNamesToV[filename], [])
        return (self.fNamesToV[filename], self.fNamesToHList[filename])

if __name__ == '__main__':
    from rpyc.utils.server import ThreadedServer
    server = ThreadedServer(MetadataStore(sys.argv[1]), port=6000)
    server.start()

