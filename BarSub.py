import zmq

class BarSub:
    def __init__(self):
        self.context = zmq.Context()
        self.receiver = self.context.socket(zmq.SUB)
        self.receiver.connect("tcp://127.0.0.1:13356")
        self.receiver.setsockopt(zmq.SUBSCRIBE, b"")
        self.poller = zmq.Poller()
        self.poller.register(self.receiver, zmq.POLLIN)

    def getdata(self):
        socks = dict(self.poller.poll(50))
        if socks:
            if socks.get(self.receiver) == zmq.POLLIN:
                message = self.receiver.recv(zmq.NOBLOCK)
                str = message.decode("utf-8")
                str_dict = eval(str)
                return str_dict
            else:
                print("error: message timeout")
                return None
        return None

