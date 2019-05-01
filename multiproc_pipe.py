import multiprocessing as mp

# TODO: use queue instead of pipe because pipe does not alow one to limit number of element that have been pushed onto the pipe



class MultiProcPipe:
    def __init__(self, head_proc):
        # Creat multiprocesses

        self.head_proc = head_proc
        proc = head_proc
        self.mp_list = []
        while proc is not None:
            self.mp_list.append(mp.Process(target=proc.run))
            self.tail_proc = proc
            proc = proc.next_proc

        # Create input connection (tx)
        self.head_proc.rx, self.tx = mp.Pipe(duplex=False)

        # Create output connection (rx)
        self.rx, self.tail_proc.tx = mp.Pipe(duplex=False)

        # Start all the processes
        for p in self.mp_list:
            p.start()
            
        # join multi processes ??

    def put(self, data):
        self.tx.send(data)

        if data is None:
            #wait for 0.01 sec, then close connection
            self.tx.close()

    def get(self):
        result = self.rx.recv()
        if result is None:
            self.rx.close()
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
        self.tx = None
        self.rx = None
        self.next_proc = None
        self.worker = worker
        # self.args = args
        

    def get(self):
        # try:
        #     val = self.rx.recv()
        # except EOFError:
        #     print('Connection closed')
        #     val = None
        res = self.rx.recv()
        if res is None:
            self.rx.close()
        return res

    def put(self, val):
        # if self.tx is None: # TODO: convert to try except block
            # return False
        self.tx.send(val)
        if val is None:
            # wait 0.1 sec, then close connection
            self.tx.close()

    def link(self, next_proc):
        self.next_proc = next_proc
        next_proc.rx, self.tx = mp.Pipe(duplex=False)

    # def close(self):
        # if self.rx:
            # self.rx.close()
        # if self.tx:
            # self.tx.close()

    def run(self):

        while True:
            x = self.get()

            if x is None:
                y = None
            else:
                y = self.worker.work(x)
            print('run', x, y)


            self.put(y)
            if x is None:
                break

        print('stage done')
            

    
    # def result(self):
        # """ Get item from queue. """



def fun(conn):
    while True:
        try:
            val = conn.recv()
        except EOFError:
            print('Connection closed')
            break
        
        if val is None:
            break
        
        print(val)
        
    conn.close()


if __name__ == '__main__':

    w1 = Worker1()
    w2 = Worker2()

    p1 = Proc(w1)
    p2 = Proc(w2)

    p1.link(p2)

    pipe = MultiProcPipe(p1)


    for i in range(10):
        print('input:', i)
        pipe.put(i)
    pipe.put(None)



    while True:
        v = pipe.get()
        print('output:', v)
        if v is None:
            break

    # mp1 = mp.Process(target=p1.run)
    # mp2 = mp.Process(target=p2.run)


    # mp2.start()
    # mp1.start()


    # mp2.join()
    # mp1.join()


    # # p1 = proc(func1, args1)
    # # p2 = proc(func2, args2)
    # # p3 = proc(func3, args3)
    # # p1.link(p2)
    # # p2.link(p3)
    # # or
    # # p1 | p2 | p3
    
    # rx_conn, tx_conn = mp.Pipe(duplex=False)

    

    # p = mp.Process(target=fun, args=(rx_conn,))
    # p.start()

    # for i in range(20):
    #     tx_conn.send((i, (i, i)))
    # # tx_conn.send(None)
    # tx_conn.close()

    # p.join()
