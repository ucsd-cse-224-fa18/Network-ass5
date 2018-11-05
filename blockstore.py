import rpyc
import sys



class BlockStore(rpyc.Service):


    def __init__(self):
        self.blocks = {}
        pass


    def exposed_store_block(self, h, block):
        self.blocks[h] = block
        for hash in h:
            print(hash)
        print(block)
        pass



    def exposed_get_block(self, h):
        return self.blocks[h]


    def exposed_has_block(self, h):
        return h in self.blocks


if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer
    port = int(sys.argv[1])
    server = ThreadPoolServer(BlockStore(), port=port)
    server.start()
