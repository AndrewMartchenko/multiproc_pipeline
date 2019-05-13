import multiprocessing as mp
from abc import ABC, abstractmethod

class BaseStage(ABC):
    __stage_count = 0 # keep track of how many processes have been created
    def __init__(self, worker, id=None):
        self._worker = worker

        if id is None:
            self.id = BaseStage.__stage_count
        else:
            self.id = id
        BaseStage.__stage_count += 1

    @abstractmethod
    def run(self):
        pass

    # @abstractmethod
    # def init2(self):
        # pass
   
class GetterStage:
    def __init__(self, id=None):
        self.rx_q = None

    def get(self):
        res = self.rx_q.get()
        if res is None:
            self.rx_q.close()
        return res

class PutterStage:
    def __init__(self, id=None):
        self.tx_q = None
        self.next_stage = None
        self.has_put = False

    def implicit_put(self, task):
        # If not explicitly put data onto queue, then put data onto queue
        if not self.has_put:
            self.tx_q.put(task)
        # Reset manually put
        self.has_put = False

    def explicit_put(self, task):
        self.tx_q.put(task)
        self.has_put = True

    def link(self, next_stage, maxqsize=100):
        if not isinstance(next_stage, GetterStage):
            raise TypeError(f'Cannot link process {self.id} to a non-getter stage {next_stage.id}.')

        self.next_stage = next_stage
        self.tx_q = mp.Queue(maxqsize)
        self.next_stage.rx_q = self.tx_q
        return self

class PipeStage(BaseStage, GetterStage, PutterStage):
    def __init__(self, worker, id=None):
        BaseStage.__init__(self, worker, id)
        GetterStage.__init__(self, id)
        PutterStage.__init__(self, id) 

    def run(self):
        while True:
            x = self.get()
            if x is None:
                self.implicit_put(None)
                break
            y = self._worker.do_work(x) # target(x, *self.args)
            self.implicit_put(y)
        print(f'stage {self.id} done')

class VoidStage(BaseStage, GetterStage):
    def __init__(self, worker, id=None):
        BaseStage.__init__(self, worker, id)
        GetterStage.__init__(self, id)
        
    def run(self):
        while True:
            x = self.get()
            if x is None:
                break
            self._worker.do_work(x) # target(x, *self.args)
        print(f'stage {self.id} done')

class GenStage(BaseStage, PutterStage):
    def __init__(self, worker, id=None):
        BaseStage.__init__(self, worker, id)
        PutterStage.__init__(self, id) 

    def run(self):
        while True:
            y = self._worker.do_work() # target(*self.args)
            self.implicit_put(y)
            if y is None:
                break
        print(f'stage {self.id} done')



        
class GenVoidStage(BaseStage):
    def __init__(self, worker, id=None):
        BaseStage.__init__(self, worker, id)
    
    def run(self):
        while True:
            y = self._worker.do_work()
            if y is None:
                break
        print(f'stage {self.id} done')

        
def stage(wobj, id=None):
    return wobj.stage(id)
