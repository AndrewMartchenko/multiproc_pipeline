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

        if self.head_proc.proc_type=='pipe':
            # Create input queue
            self.tx_q = mp.Queue(maxsize=maxqsize)
            self.head_proc.rx_q = self.tx_q
        elif self.head_proc.proc_type=='generator':
            self.tx_q = None
        else:
            assert False, 'Head process must be of type pipe or generator'
        
        if self.tail_proc=='no_output':
            self.rx_q = None
        else: # 'pipe', or 'generator'
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
    return isinstance(obj, types.LambdaType) and obj.__name__ == '<lambda>'


class Proc:
    __proc_count = 0 # keep track of how many processes have been created
    def __init__(self, target, args = (), proc_type='pipe', id=None):
        assert not is_lambda(target), 'target cannot be a lambda function.'
        # Pipe connections
        self.tx_q = None
        self.rx_q = None
        self.next_proc = None
        self.proc_type = proc_type
        self.target = target
        self.args = args


        if id is None:
            self.id = Proc.__proc_count
        else:
            self.id = id
        Proc.__proc_count += 1

        
        if self.proc_type == 'generator':
            self.run = self.run_generator_process
        elif self.proc_type == 'no_output': # void?
            self.run = self.run_no_output_process
        else:
            self.run = self.run_pipe_process


    def get(self):
        res = self.rx_q.get()
        if res is None:
            self.rx_q.close()
        return res

    def put(self, val):
        self.tx_q.put(val)

    def link(self, next_proc, maxqsize=100):
        assert  next_proc.proc_type != 'generator', f'Cannot link process {self.id} to generator process {next_proc.id}.'
        assert self.proc_type != 'no_output', f'No_Output process {self.id} cannot link onto another process'
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

    def run_no_output_process(self):
        while True:
            x = self.get()

            if x is None:
                self.put(None)
                break
            self.target(x, *self.args)
        print(f'stage {self.id} done')


def func(x, pwr):
    return x**pwr

# The following class is intended to be an interface to some other class.
# The interface will manage locks so that object can run in a multiprocess.
class GenGen:
    def __init__(self, n):
        self.n = n
        self.x = 0

    def work(self):

        if self.x > self.n:
            return None
        self.x += 1
        return self.x

def void_func(x, *args):
    print(x)

if __name__ == '__main__':

    gen = GenGen(10)
    p0 = Proc(target=gen.work, proc_type='generator')
    p1 = Proc(target=func, args=(2,))
    p2 = Proc(target=func, args=(2,))
    p3 = Proc(target=func, args=(2,))
    p4 = Proc(target=func, args=(2,))
    p5 = Proc(target=void_func, args=(3,), proc_type='no_output')

    p0.link(p1.link(p2.link(p3.link(p4.link(p5)))))
    # TODO: add merge and split

    pipe = MultiprocPipe(p0)

    # # Put values in pipe
    # for i in range(10):
    #     print('input:', i)
    #     pipe.put(i)
    # pipe.put(None)


    # # Get results from pipe
    # while True:
    #     v = pipe.get()
    #     # print('output:', v)
    #     if v is None:
    #         break

