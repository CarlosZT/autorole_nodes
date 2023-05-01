import socket
class Seeker():
    def __init__(self) -> None:
        self.master_port = 5001
        self.seekers_port = 5000
        self.any_ip = '0.0.0.0'

        self.listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind((self.any_ip, self.seekers_port))

        self.sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sender.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def reveal_master(self, tries = 10):
        msg = 'MASTER?'
        utils = None
        master_addr = None

        for i in range(tries):
            self.sender.sendto(msg.encode(), ('<broadcast>', self.master_port))
            out_port = self.sender.getsockname()[1]
            self.listener.settimeout(1)
            try:
                data, addr = self.listener.recvfrom(1024)

                if addr[1] != out_port:
                    recv_msg = data.decode()
                    tokens = recv_msg.split(';')

                    # print(f'[{i}] Incoming data: {recv_msg}, from: {addr}')
                    if tokens[0] == 'MASTER_OK':
                        print(f'[{i}] Master founded')
                        master_addr = addr
                        utils = [tokens[1], master_addr, tokens[2]]
                        break
            except socket.timeout:
                print(f'[{i}] Timed out. Retrying...')

        self.listener.close()
        self.sender.close()

        return utils