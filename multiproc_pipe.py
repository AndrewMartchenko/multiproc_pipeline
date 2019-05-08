import multiprocessing as mp
import types
from abc import ABC, abstractmethod

# TODO: add merge and split

class MultiprocPipe:
    def __init__(self, head_stage, maxqsize=100):
        # Creat multiprocesses
        self.head_stage = head_stage
        stage = head_stage
        self.mp_list = []
        while stage is not None:
            self.mp_list.append(mp.Process(target=stage.run))
            self.tail_stage = stage
            if not isinstance(stage, PutterStage):
                break
            stage = stage.next_stage

        if isinstance(self.head_stage, GetterStage):
            # Create input queue
            self.tx_q = mp.Queue(maxsize=maxqsize)
            self.head_stage.rx_q = self.tx_q
        else:
            self.tx_q = None
        
        if isinstance(self.tail_stage, PutterStage):
            # Create output queue
            self.rx_q = mp.Queue(maxsize=maxqsize)
            self.tail_stage.tx_q = self.rx_q
        else:
            self.rx_q = None

    def start(self):
        # Start all the processes
        for p in self.mp_list:
            p.start()

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

def Stage(target, args=(), is_input=True, is_output=True, id=None):
    """Factory function used to build pipeline stage objects."""
    if is_input and is_output:
        return PipeStage(target, args, id)
    if is_input and not is_output:
        return VoidStage(target, args, id)
    if not is_input and is_output:
        return GenStage(target, args, id)
    if not is_input and not is_output:
        return GenVoidStage(target, args, id)


class BaseStage(ABC):
    __stage_count = 0 # keep track of how many processes have been created
    def __init__(self, target, args=(), id=None):
        assert not is_lambda(target), 'target cannot be a lambda function.'
        self.target = target
        self.args = args

        if id is None:
            self.id = BaseStage.__stage_count
        else:
            self.id = id
        BaseStage.__stage_count += 1

    @abstractmethod
    def run(self):
        pass

class GetterStage(BaseStage):
    def __init__(self, target, args=(), id=None):
        BaseStage.__init__(self, target, args, id)
        self.rx_q = None

    def get(self):
        res = self.rx_q.get()
        if res is None:
            self.rx_q.close()
        return res

class PutterStage(BaseStage):
    def __init__(self, target, args=(), id=None):
        BaseStage.__init__(self, target, args, id)
        self.tx_q = None
        self.next_stage = None

    def put(self, val):
        self.tx_q.put(val)

    def link(self, next_stage, maxqsize=100):
        if not isinstance(next_stage, GetterStage):
            raise TypeError(f'Cannot link process {self.id} to a non-getter stage {next_stage.id}.')

        self.next_stage = next_stage
        self.tx_q = mp.Queue(maxqsize)
        self.next_stage.rx_q = self.tx_q
        return self
    

class PipeStage(GetterStage, PutterStage):
    def __init__(self, target, args=(), id=None):
        GetterStage.__init__(self, target, args, id)
        PutterStage.__init__(self, target, args, id) # Base class constructor will be called twice

    def run(self):
        while True:
            x = self.get()
            if x is None:
                self.put(None)
                break
            y = self.target(x, *self.args)
            self.put(y)
        print(f'stage {self.id} done')

class VoidStage(GetterStage):
    def __init__(self, target, args=(), id=None):
        GetterStage.__init__(self, target, args, id)

    def run(self):
        while True:
            x = self.get()
            if x is None:
                break
            self.target(x, *self.args)
        print(f'stage {self.id} done')

class GenStage(PutterStage):
    def __init__(self, target, args=(), id=None):
        PutterStage.__init__(self, target, args, id)

    def run(self):
        while True:
            y = self.target(*self.args)
            self.put(y)
            if y is None:
                break
        print(f'stage {self.id} done')

class GenVoidStage(BaseStage):
    def __init__(self, target, args=(), id=None):
        BaseStage.__init__(self, target, args, id)

    def run(self):
        while True:
            y = self.target(*self.args)
            if y is None:
                break
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

class GenVoid:
    def __init__(self, n):
        self.n = n
        self.x = 0

    def work(self):
        print(self.x)
        if self.x > self.n:
            return None
        self.x += 1
        return self.x

def void_func(x, *args):
    print(x)

if __name__ == '__main__':

    # Test only one GenVoid pipline/stage
    gen_void = GenVoid(10)
    p0 = Stage(target=gen_void.work, is_input=False, is_output=False, id='gen_void_1')
    pipe = MultiprocPipe(p0)

    print('Test 1 done.')

    # Test Geng -> Pipe -> Void pipeline
    gen = GenGen(10)
    p0 = Stage(target=gen.work, is_input=False)
    p1 = Stage(target=func, args=(2,))
    p2 = Stage(target=func, args=(2,))
    p3 = Stage(target=func, args=(2,))
    p4 = Stage(target=func, args=(2,))
    p5 = Stage(target=void_func, args=(3,), is_output=False)

    
    p0.link(p1)
    p1.link(p2)
    p2.link(p3)
    p3.link(p4)
    p4.link(p5)

    pipe = MultiprocPipe(p0)
    pipe.start()
    print('Test 2 done.')

    # Test Pipe -> Pipe -> Pipe pipeline
    p1 = Stage(target=func, args=(2,))
    p2 = Stage(target=func, args=(2,))
    p3 = Stage(target=func, args=(2,))
    p4 = Stage(target=func, args=(2,))

    p1.link(p2)
    p2.link(p3)
    p3.link(p4)

    pipe = MultiprocPipe(p1)
    pipe.start()
    
    # Put values in pipe
    for i in range(10):
        print('input:', i)
        pipe.put(i)
    pipe.put(None)

    print('Test 3 done.')

    # Get results from pipe
    while True:
        v = pipe.get()
        # print('output:', v)
        if v is None:
            break



    # This is what split should look like
    # p0.link(p1a.link(p1b.link(p1c)),
    #         p2a.link(p2b.link(p2c)),
    #         p3a.link(p3b.link(p3c)))

    # This is what merge would look like
    # p4.merge(p1c, p2c, p3c)



