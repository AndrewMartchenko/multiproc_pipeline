import multiprocessing as mp
import types


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

        if not self.head_proc.is_generator:
            # Create input queue
            self.tx_q = mp.Queue(maxsize=maxqsize)
            self.head_proc.rx_q = self.tx_q
        else:
            self.tx_q = None
            print('here')

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


def is_lambda(obj):
    return isinstance(obj, types.LambdaType) and obj.__name__ == "<lambda>"


class Proc:
    __proc_count = 0 # keep track of how many processes have been created
    def __init__(self, target, args, is_generator=False, id=None):
        assert not is_lambda(target), 'target cannot be a lambda function.'
        # Pipe connections
        self.tx_q = None
        self.rx_q = None
        self.next_proc = None
        self.is_generator = is_generator
        self.target = target
        self.args = args

        if self.is_generator:
            self.run = self.run_generator_process
        else:
            self.run = self.run_pipe_process
           

        if id is None:
            self.id = Proc.__proc_count
        else:
            self.id = id
        Proc.__proc_count += 1

    def get(self):
        res = self.rx_q.get()
        if res is None:
            self.rx_q.close()
        return res

    def put(self, val):
        self.tx_q.put(val)

    def link(self, next_proc, maxqsize=100):
        assert not next_proc.is_generator, f'Cannot link process {self.id} to generator process {next_proc.id}.'
        self.next_proc = next_proc
        self.tx_q = mp.Queue(maxqsize)
        self.next_proc.rx_q = self.tx_q
        return self

    def run_generator_process(self):
        while True:
            y = self.target(*self.args)
            self.put(y)
            if y is None:
                break;
        print(f'stage {self.id} done')

    def run_pipe_process(self):
        while True:
            x = self.get()

            if x is None:
                self.put(None)
                break
            # y = self.target(x)
            y = self.target(x, *self.args)
            self.put(y)
        print(f'stage {self.id} done')


def f0(lst):

    x = lst[0]
    print(x)
    if x > 10:
        return None
    lst[0] += 1
    return x

def f1(x, *args):
    return x+1

def f2(x, *args):
    return x*2

def f3(x, *args):
    return x**2

def f4(x, *args):
    return x-3

class GenGen:
    def __init__(self):
        self.x = 0

    def work(self):

        print(self.x)
        if self.x > 10:
            return None
        self.x += 1
        return self.x


if __name__ == '__main__':

    x = 1
    gen = GenGen()
    p0 = Proc(target=gen.work, args=(), is_generator=True)
    p1 = Proc(target=f1, args=())
    p2 = Proc(target=f1, args=())
    p3 = Proc(target=f1, args=())
    p4 = Proc(target=f1, args=())

    p0.link(p1.link(p2.link(p3.link(p4))))
    # TODO: add merge and split

    pipe = MultiprocPipe(p0)

    # # Put values in pipe
    # for i in range(10):
    #     print('input:', i)
    #     pipe.put(i)
    # pipe.put(None)


    # Get results from pipe
    while True:
        v = pipe.get()
        print('output:', v)
        if v is None:
            break

