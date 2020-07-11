# from __init__ import *
from pipeline import Pipeline
from stage import PipeStage, VoidStage, GenVoidStage, GenStage, stage
from worker import PipeWorker, VoidWorker, GenVoidWorker, GenWorker



# TODO:
# - target function in stages needs to be replaced with an object so
#   that it can push data onto the queue
# - add split
# - add merge
# - allow multiple processes per stage


# def is_lambda(obj):
    # return isinstance(obj, types.LambdaType) and obj.__name__ == '<lambda>'


# Note: I don't like these overloaded worker classes as they require knowlede of the
# internal workings of the variouse worker classes.

class MyWorkerClass(GenVoidWorker): # or GenWorker or GenVoidWorker or PipeWorker
    def __init__(self, n):
        self.n = n
        self.x = 0
    
    def do_work(self):
        print(self.x)
        if self.x > self.n:
            return None
        self.x += 1
        return self.x 

class Square(PipeWorker):
    def do_work(self, x):
        return x*x # return will implicitly put the result onto queue


class Square2(PipeWorker):
    def do_work(self, x):
        self.put(x*x) # self.put will explicitly put result onto queue as many times as you like
        self.put(x*x)

def f(lst):
    x = lst[0]
    if x < 10:
        print(f'f: {x}')
        lst[0] += 1
        return x
    return None

    
if __name__ == '__main__':

    # New simplified version
    # ======================

    # There will be no distinction between workers and stages

    # A stage will contain
    # 1. A function to execute. Return type will be pushed onto the next stage
    # 2. Number of processes
    # 3. flag ordered or unordered stage

    # Pipeline structure will determine wether a stage is a gen, pipe, or void stage

    # e.g.
    #        -- s1 --                                                             
    #       /        \                                                           
    # s0 --+--- s2 ---+-- s4                                                   
    #       \                                                                  
    #        -- s3 --N
    
    s0 = stage(func=f0, nproc=5, stage_type='ordered', push_out=True)
    s1 = stage(func=f1, nproc=5, stage_type='ordered', push_out=True)
    s2 = stage(func=f2, nproc=5, stage_type='ordered', push_out=True)
    s3 = stage(func=f3, nproc=5, stage_type='ordered', push_out=False)
    s4 = stage(func=f4, nproc=5, stage_type='ordered', push_out=True)

    s0.link(s1, s2, s3)
    s3.merge(s1, s2)
    
    












    
    # How do I want to use the class

    # 1. Define worker worker object
    # w = MyWorkerClass(20)


    # worker = MyWorkerClass(20)
    # print(worker)
    # p0 = GenVoidStage(worker)
    # pipe = Pipeline(p0)
    # pipe.start()
    

    # # Test only one GenVoid pipline/stage

    # worker = MyWorkerClass(20)
    x = 0
    worker = GenVoidWorker(f, args=([x,],))
    p0 = stage(worker)
    print(p0)
    pipe = Pipeline(p0)
    pipe.start()

    print('Test 1 done.')

    # # Test Geng -> Pipe -> Void pipeline
    # gen = GenGen(10)
    # p0 = GenWorker(target=gen.work, is_input=False)
    # p1 = Stage(target=func, args=(2,))
    # p2 = Stage(target=func, args=(2,))
    # p3 = Stage(target=func, args=(2,))
    # p4 = Stage(target=func, args=(2,))
    # p5 = Stage(target=void_func, args=(3,), is_output=False)

    # p0.link(p1)
    # p1.link(p2)
    # p2.link(p3)
    # p3.link(p4)
    # p4.link(p5)

    # pipe = Pipeline(p0)
    # pipe.start()
    # print('Test 2 done.')

    # Test Pipe -> Pipe -> Pipe pipeline
    w1 = Square2()
    w2 = Square()
    w3 = Square()
    w4 = Square()
    print(w1)
    p1 = stage(w1)
    p2 = stage(w2)
    p3 = stage(w3)
    p4 = stage(w4)

    p1.link(p2)
    p2.link(p3)
    p3.link(p4)

    pipe = Pipeline(p1)
    pipe.start()
    
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

    print('Test 3 done.')

    # This is what split should look like
    # p0.link(p1a.link(p1b.link(p1c)),
    #         p2a.link(p2b.link(p2c)),
    #         p3a.link(p3b.link(p3c)))

    # This is what merge would look like
    # p4.merge(p1c, p2c, p3c)
