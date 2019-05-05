import multiprocessing as mp

# TODO: use queue instead of pipe because pipe does not alow one to limit number of element that have been pushed onto the pipe



class MultiprocPipe:
    def __init__(self, head_proc, maxqsize=100):
        # Creat multiprocesses

        self.head_proc = head_proc
        proc = head_proc
        self.mp_list = []
        while proc is not None:
            self.mp_list.append(mp.Process(target=proc.run))
            self.tail_proc = proc
            proc = proc.next_proc

        # Create input queue
        self.tx_q = mp.Queue(maxsize=maxqsize)
        self.head_proc.rx_q = self.tx_q

        # Create output queue
        self.rx_q = mp.Queue(maxsize=maxqsize)
        self.tail_proc.tx_q = self.rx_q

        # Start all the processes
        for p in self.mp_list:
            p.start()
            
        # join multi processes ??

    def put(self, data):
        self.tx_q.put(data)

        # Responsibility of self.get function to close
        # pipe when None is received!

    def get(self, block=True, timeout=None):
        result = self.rx_q.get(block, timeout)
        if result is None:
            self.rx_q.close()
        return result


# This needs to be abstract class
class Worker1:
    def __init__(self):
        pass

    def work(self, x):
        return x*10


class Worker2:
    def __init__(self):
        pass

    def work(self, x):
        return x+1

class Proc:
    def __init__(self, worker):
        # Pipe connections
        self.tx_q = None
        self.rx_q = None
        self.next_proc = None
        self.worker = worker

    def get(self):
        res = self.rx_q.get()
        if res is None:
            self.rx_q.close()
        return res

    def put(self, val):
        self.tx_q.put(val)

    def link(self, next_proc, maxqsize=100):
        self.next_proc = next_proc
        self.tx_q = mp.Queue(maxqsize)
        self.next_proc.rx_q = self.tx_q
        return self



    def run(self):

        while True:
            x = self.get()

            if x is None:
                self.put(None)
                break
            y = self.worker.work(x)
            self.put(y)

        print('stage done')


if __name__ == '__main__':

    w1 = Worker1()
    w2 = Worker2()
    w3 = Worker1()
    w4 = Worker2()

    p1 = Proc(w1)
    p2 = Proc(w2)
    p3 = Proc(w3)
    p4 = Proc(w4)

    p1.link(p2.link(p3.link(p4)))


    pipe = MultiprocPipe(p1)


    # Put values in pipe
    for i in range(10):
        print('input:', i)
        pipe.put(i)
    pipe.put(None)


    # Get results from pipe
    while True:
        v = pipe.get()
        print('output:', v)
        if v is None:
            break

