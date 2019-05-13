from abc import ABC, abstractmethod
from stage import GenVoidStage, GenStage, VoidStage, PipeStage

class Worker(ABC):
    def __init__(self):
        self.__stage_obj = None

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
    def put(self, x):
        self.stage_obj.explicit_put(x)

class GenVoidWorker(Worker):
    def stage(self, id=None):
        self.stage_builder(GenVoidStage, id)
        return self.stage_obj

class GenWorker(PutterWorker):
    def stage(self, id=None):
        self.stage_builder(GenStage, id)
        return self.stage_obj

class VoidWorker(Worker):
    def stage(self, id=None):
        self.stage_builder(VoidStage, id)
        return self.stage_obj

class PipeWorker(PutterWorker):
    def stage(self, id=None):
        self.stage_builder(PipeStage, id)
        return self.stage_obj

