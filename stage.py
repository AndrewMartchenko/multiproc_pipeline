import multiprocessing as mp

class BaseStage():
    __stage_count = 0 # keep track of how many processes have been created
    def __init__(self, worker, id=None):
        self._worker = worker

        if id is None:
            self.id = BaseStage.__stage_count
        else:
            self.id = id
        BaseStage.__stage_count += 1

class PutterStageComponent:
    def __init__(self):
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
        if not isinstance(next_stage, (VoidStage, PipeStage)):
            raise TypeError(f'Cannot link process {self.id} to a non-getter stage {next_stage.id}.')

        self.next_stage = next_stage
        self.tx_q = mp.Queue(maxqsize)
        self.next_stage.rx_q = self.tx_q
        return self

   
class GetterStageComponent:
    def __init__(self):
        self.rx_q = None

    def get(self):
        res = self.rx_q.get()
        if res is None:
            self.rx_q.close()
        return res


class VoidStage(BaseStage):
    def __init__(self, worker, id=None):
        BaseStage.__init__(self, worker, id) # init base class
        self.getter_obj = GetterStageComponent()

    def get(self):
        return self.getter_obj.get()

    @property
    def rx_q(self):
        return self.getter_obj.rx_q


    @rx_q.setter
    def rx_q(self, rx_q):
        self.getter_obj.rx_q = rx_q

        
    def run(self):
        while True:
            x = self.get()
            if x is None:
                break
            self._worker.do_work(x) # target(x, *self.args)
        print(f'stage {self.id} done')

class GenStage():
    def __init__(self, worker, id=None):
        BaseStage.__init__(self, worker, id)
        self.putter_obj = PutterStageComponent()

    def implicit_put(self, task):
        self.putter_obj.implicit_put(task)

    def explicit_put(self, task):
        self.putter_obj.explicit_put(task)

    def link(self, next_stage, maxqsize=100):
        self.putter_obj.link(next_stage, maxqsize)

    @property
    def next_stage(self):
        return self.putter_obj.next_stage

    @property
    def tx_q(self):
        return self.putter_obj.tx_q

    @tx_q.setter
    def tx_q(self, tx_q):
        self.putter_obj.tx_q = tx_q



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
            y = self._worker.do_work(None)
            if y is None:
                break
        print(f'stage {self.id} done')


class PipeStage():
    def __init__(self, worker, id=None):
        BaseStage.__init__(self, worker, id) # init base class
        self.getter_obj = GetterStageComponent()
        self.putter_obj = PutterStageComponent()

    def get(self):
        return self.getter_obj.get()

    def implicit_put(self, task):
        self.putter_obj.implicit_put(task)

    def explicit_put(self, task):
        self.putter_obj.explicit_put(task)

    def link(self, next_stage, maxqsize=100):
        self.putter_obj.link(next_stage, maxqsize)

    @property
    def next_stage(self):
        return self.putter_obj.next_stage

    @property
    def tx_q(self):
        return self.putter_obj.tx_q

    @tx_q.setter
    def tx_q(self, tx_q):
        self.putter_obj.tx_q = tx_q

    @property
    def rx_q(self):
        return self.getter_obj.rx_q

    @rx_q.setter
    def rx_q(self, rx_q):
        self.getter_obj.rx_q = rx_q


    def run(self):
        while True:
            x = self.get()
            if x is None:
                self.implicit_put(None)
                break
            y = self._worker.do_work(x) # target(x, *self.args)
            self.implicit_put(y)
        print(f'stage {self.id} done')

    
        
def stage(wobj, id=None):
    return wobj.stage(id)
