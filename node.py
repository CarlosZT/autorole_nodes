from master import *
import  xmlrpc.server as rpc_server 

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP



class Node():
    def __init__(self, utils, port=8000, hb_port = 8001, hb_time=3) -> None:

        self.utils = utils
        self.master_address = utils[1]
        self.master_token = utils[2]
        self.node_address = (utils[0], port)
        self.hb_address = (utils[0], hb_port)
        self.any_ip = '0.0.0.0'
        self.hb_time = hb_time
        self.broadcast_port = 5000

        self.role_state = 0

        # self.rand_tx_times = 3
  
        self.sched_id = None
        self.name = None
        self.alive = threading.Thread(target=self.dummy_process)
        self.dummy_flag = True
        self.alive.start()

        self.hb_service = threading.Thread(target=self.heartbeat_service)

        self.missing_alert = threading.Timer(hb_time*3, self.master_missing)     
         
        print(f'[{self.name}] Submitting...')
        self.master = Pyro4.Proxy(self.master_token)
        # self.name = self.master.submit(self.node_address, self.hb_address)

        self.name = self.master.submit(self.hb_address)

        if self.name:
            print(f'[{self.name}] Submission succesfully')
            self.hb_service.start()
            print(f'[{self.name}] Heartbeat service is running at {self.hb_address}')
        else:
            print(f'[{self.name}] Connection refused. Possible addresses conflict. Shutting down...')
            self.dummy_flag = False
            self.stop_services()
            exit()

    def dummy_process(self):
        while self.dummy_flag:
            pass

    
    def heartbeat_service(self):
        try:
            self.hb_rpc= rpc_server.SimpleXMLRPCServer(self.hb_address)
            self.hb_rpc.register_function(self.token_auth, 'token_auth')
            self.hb_rpc.serve_forever()
        except: pass


    def stop_services(self):
        try:
            self.hb_rpc.server_close()
            # self.hb_rpc.shutdown()
            self.hb_service.join()        
            self.missing_alert.cancel()
        except: pass


    def token_auth(self, token):
        self.missing_alert.cancel()
        self.missing_alert = threading.Timer(self.hb_time*3, self.master_missing)     

        self.missing_alert.start()
        print(f'[{self.name}] Heartbeat received')

        return str(self.master_token) == str(token)
    
    def listen_randoms(self, n):
        print(f'[{self.name}] Waiting for other numbers...')

        lstnr = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        lstnr.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lstnr.bind((self.any_ip, self.broadcast_port))
        self.role_state = 0
        tries = 2
        i = 0
        while True:
            lstnr.settimeout(1)
            try:
                data, addr = lstnr.recvfrom(1024)
                recv_msg = data.decode()
                recv_msg = recv_msg.split(';')
                name = recv_msg[0]
                if name != self.name:
                    
                    n_recv = float(recv_msg[1])
                    if n_recv > n:
                        print(f'[{self.name}] Smallest than {name}: {n, n_recv}')
                        self.role_state = 0
                        break
                    else:
                        print(f'[{self.name}] Bigger than {name}: {n, n_recv}')
                        self.role_state = 1
                        
                        
            except socket.timeout:
                print(f'[{self.name}] Timeout')
                if i < tries:
                    self.make_broadcast(f'{self.name};{str(n)}')
                    i+= 1
                else:
                    self.role_state = 1
                    break
        lstnr.close()
        
            
    def make_broadcast(self, msg:str):
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sender.sendto(msg.encode(), ('<broadcast>', self.broadcast_port))
        sender.close()


    def master_missing(self):
        #No podemos recuperar al master dado que no existe persistencia de datos aun.
        print(f'[{self.name}] MASTER MISSING')
        try:
            self.stop_services()
        except: pass

        #Generamos el numero aleatorio
        n = np.random.random()
        print(f'[{self.name}] Generated: {n}')

        #Abrimos puertos de broadcast
        listener = threading.Thread(target=lambda: self.listen_randoms(n))
        listener.start()

        #Enviamos por broadcast el numero
        print(f'[{self.name}] Sending my number...')
        self.make_broadcast(f'{self.name};{str(n)}')

        listener.join()
        
        if self.role_state == 1:
            print(f'[{self.name}] Ill be master')
            # main([self.role_state])
        else:
            print(f'[{self.name}] Ill be Node')
            # main([self.role_state, self.node_address[1], self.hb_address[1], self.utils])
        self.dummy_flag = False