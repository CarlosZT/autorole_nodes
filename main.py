from node import Seeker, Master, time, Node    

def main():
    seeker = Seeker()
    utils = seeker.reveal_master(tries = 10)
    
    if not utils:
        node = Master()
    else:
        port = 8008
       
        node = Node(utils=utils, port=port, hb_port=port*2)
        node.alive.join()

        while True:
            if node.role_state == 0:
                time.sleep(2)
                seeker = Seeker()
                utils = seeker.reveal_master()
                node = Node(utils=utils, port=node.node_address[1], hb_port=node.hb_address[1])
                node.alive.join()
            else:
                node = Master()
                break


if __name__ == '__main__':
    main()