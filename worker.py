from abc import ABC, abstractmethod
from stage import GenVoidStage, GenStage, VoidStage, PipeStage

class Worker(ABC):
    # def __init__(self):
    def __init__(self, target, args=()):
        self.__stage_obj = None
        # Create default worker
        self._target = target
        self._args = args



    def stage_builder(self, StageClass, id):
        self.__stage_obj = StageClass(self, id)

    @property
    def stage_obj(self):
        return self.__stage_obj
        
    @abstractmethod
    def stage(self, id):
        pass

    @abstractmethod
    def do_work(self):
        pass

class PutterWorker(Worker):
    def __init__(self):
        pass
    def put(self, x):
        self.stage_obj.explicit_put(x)

class GenVoidWorker(Worker):
    def stage(self, id=None):
        self.stage_builder(GenVoidStage, id)
        return self.stage_obj
    
    def do_work(self):
        return self._target(*self._args)

class GenWorker(PutterWorker):
    def stage(self, id=None):
        self.stage_builder(GenStage, id)
        return self.stage_obj
    
    def do_work(self):
        return self._target(x, *self._args)

class VoidWorker(Worker):
    def stage(self, id=None):
        self.stage_builder(VoidStage, id)
        return self.stage_obj
    def do_work(self, x):
        return self._target(x, *self._args)

class PipeWorker(PutterWorker):
    def stage(self, id=None):
        self.stage_builder(PipeStage, id)
        return self.stage_obj

    def do_work(self, x):
        return self._target(x, *self._args)


# So far there are 3 functions that a stage can perform
# 1) generate data and 
# 2) process and output
# 3) process and void

