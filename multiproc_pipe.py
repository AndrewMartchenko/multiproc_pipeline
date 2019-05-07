import multiprocessing as mp
import types

class MultiprocPipe:
    def __init__(self, head_stage, maxqsize=100):
        # Creat multiprocesses
        self.head_stage = head_stage
        stage = head_stage
        self.mp_list = []
        while stage is not None:
            self.mp_list.append(mp.Process(target=stage.run))
            self.tail_stage = stage
            stage = stage.next_stage

        # Head process should not be no_output stage, because then the pipe would be no diferent to a si

        if type(self.head_stage) is PipeStage:
            # Create input queue
            self.tx_q = mp.Queue(maxsize=maxqsize)
            self.head_stage.rx_q = self.tx_q
        elif type(self.head_stage) is GenStage:
            self.tx_q = None
        else:
            assert False, 'Head process must be of type pipe or generator'
        
        if type(self.tail_stage) is VoidStage:
            self.rx_q = None
        else: # INPUT_OUTPUT, or NO_INPUT
            # Create output queue
            self.rx_q = mp.Queue(maxsize=maxqsize)
            self.tail_stage.tx_q = self.rx_q
        
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
    raise Warning('Stage process must have at least one input or output. If you only have one process and not input or output queue, then just use a multiprocessing - you do not need a Pipeline.')

class PipeStage:
    __stage_count = 0 # keep track of how many processes have been created
    def __init__(self, target, args=(), id=None):
        assert not is_lambda(target), 'target cannot be a lambda function.'
        self.target = target
        self.args = args
        self.tx_q = None
        self.rx_q = None
        self.next_stage = None

        if id is None:
            self.id = PipeStage.__stage_count
        else:
            self.id = id
        PipeStage.__stage_count += 1

    def get(self):
        res = self.rx_q.get()
        if res is None:
            self.rx_q.close()
        return res

    def put(self, val):
        self.tx_q.put(val)

    def link(self, next_stage, maxqsize=100):
        if type(next_stage) is GenStage:
            raise TypeError(f'Cannot link process {self.id} to generator process {next_stage.id}.')

        self.next_stage = next_stage
        self.tx_q = mp.Queue(maxqsize)
        self.next_stage.rx_q = self.tx_q
        return self

    def run(self):
        while True:
            x = self.get()
            if x is None:
                self.put(None)
                break
            y = self.target(x, *self.args)
            self.put(y)
        print(f'stage {self.id} done')

    def _raise_type_error(self, method_name):
        raise TypeError(f'"{method_name}" method is not definded for object of type "{type(self).__name__}"')
        

        
class VoidStage(PipeStage):
    def __init__(self, target, args=(), id=None):
        PipeStage.__init__(self, target, args, id)
        del(self.tx_q)

    def put(self, val):
        self._raise_type_error('put')

    def link(self, next_stage, maxqsize=100):
        self._raise_type_error('link')

    def run(self):
        while True:
            x = self.get()
            if x is None:
                break
            self.target(x, *self.args)
        print(f'stage {self.id} done')

class GenStage(PipeStage):
    def __init__(self, target, args=(), id=None):
        PipeStage.__init__(self, target, args, id)
        self.tx_q = None
        self.next_stage = None
        # This stage cannot receive inputs. rx_q not required
        del(self.rx_q)

    def get(self):
        self._raise_type_error('get')

    def run(self):
        while True:
            y = self.target(*self.args)
            self.put(y)
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

def void_func(x, *args):
    print(x)

if __name__ == '__main__':

    gen = GenGen(10)
    p0 = Stage(target=gen.work, is_input=False)
    p1 = Stage(target=func, args=(2,))
    p2 = Stage(target=func, args=(2,))
    p3 = Stage(target=func, args=(2,))
    p4 = Stage(target=func, args=(2,))
    p5 = Stage(target=void_func, args=(3,), is_output=False)

    p0.link(p1.link(p2.link(p3.link(p4.link(p5)))))
    # TODO: add merge and split

    # This is what split should look like
    # p0.link(p1a.link(p1b.link(p1c)),
    #         p2a.link(p2b.link(p2c)),
    #         p3a.link(p3b.link(p3c)))

    # This is what merge would look like
    # p4.merge(p1c, p2c, p3c)
    

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

