import Pyro4
import xmlrpc.client as rpc_client
import time
import threading
import numpy as np
from seeker import *

@Pyro4.expose
class Master():
    def __init__(self) -> None:
        self.master_port = 5001
        self.seekers_port = 5000
        self.any_ip = '0.0.0.0'
        self.nodes = []
        self.hb_addresses = []
        self.hb_time = 3

        self.refused_connections = {}
        self.max_refuse = 3


        #Submit service
        self.sub_thread = threading.Thread(target=self.submit_service)
        self.sub_thread.start()
        
        
        #Seekers manager service
        self.sm_thread = threading.Thread(target=self.seekers_manager)
        time.sleep(1)
        self.sm_thread.start()

        #Heartbeat service
        self.hb_thread = threading.Thread(target=self.heartbeat_check)
        time.sleep(1)
        self.hb_thread.start()

        print('[Master] Ready!')


    
    def seekers_manager(self):
        seekers_lstnr = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        seekers_lstnr.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        seekers_lstnr.bind((self.any_ip, self.master_port))

        seeker_resp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        seeker_resp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        print('[Master] Listening seekers')

        while True:
            data, addr = seekers_lstnr.recvfrom(1024)
            recv_msg = data.decode()
            # print(f'[Master] Received: {recv_msg}, from: {addr}')

            if recv_msg == 'MASTER?':
                
                print(f'[Master] Announcing to {addr}')
                msg = f'MASTER_OK;{addr[0]};{self.uri}'

                seeker_resp.sendto(msg.encode(), (addr[0], self.seekers_port))


    
    def submit(self, hb_address):
        if not hb_address in self.hb_addresses:
            name = 'Node_' + str(np.random.random())[2:]
            self.nodes.append((name, hb_address))
            self.hb_addresses.append(hb_address)
            self.refused_connections[str(hb_address)] = 0            
                    
        
            print(f'[Master] Registered: {name}, Heartbeat: {hb_address}')
            return name
        else:
            if self.refused_connections[str(hb_address)] > 0:
                i = self.hb_addresses.index(hb_address)
                name = self.nodes[i][0]
                self.refused_connections[str(hb_address)] = 0
                print(f'[Master] Recovered: {name}, Heartbeat: {hb_address}')
                return name

            print(f'[Master] Refused: Heartbeat address({hb_address}) already registered. Same device?')
            return None
    
    
    def submit_service(self):
        self.daemon = Pyro4.Daemon()
        self.uri = self.daemon.register(self)
        self.daemon.requestLoop()


    
    def heartbeat_check(self):
        to_remove = []
        print('[Master] Trying heartbeat')
        result = None
        while True:
                    
            for addr, node in zip(self.hb_addresses, self.nodes):

                with rpc_client.ServerProxy(f'http://{addr[0]}:{addr[1]}/', allow_none=True) as proxy:
                    
                    try:
                        result = proxy.token_auth(str(self.uri))
                        if result != None:
                            if result:
                                print(f'[Master] OK: {node[0]}')
                            else:
                                print(f'[Master] Authentication failed: {node}')
                        else:
                            print(f'[Master] None result from: {node}')
                    except Exception as e: 
                        print(f'[Master] Exception: Cant\'t connect to node')
                        if  self.refused_connections[str(addr)] < self.max_refuse:
                            self.refused_connections[str(addr)] += 1
                            print(f'[Master] Refused connections by: {node[0]} => {self.refused_connections[str(addr)]}')
                        else:
                            print(f'[Master] Maximum refused connections reached. Removing {node[0]}')
                            to_remove.append((addr, node))
                            self.refused_connections.pop(str(addr))
                            
            
                result = None
                    # print('[Master] Ok!')

            for e in to_remove:


                self.hb_addresses.remove(e[0])
                self.nodes.remove(e[1])

            to_remove = []

            time.sleep(self.hb_time)
