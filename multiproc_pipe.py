# from __init__ import *
from pipeline import Pipeline
from stage import Stage, PipeStage, VoidStage, GenVoidStage, GenStage, stage
import time
# from worker import PipeWorker, VoidWorker, GenVoidWorker, GenWorker



# TODO:
# - target function in stages needs to be replaced with an object so
#   that it can push data onto the queue
# - add split
# - add merge
# - allow multiple processes per stage


def f(lst):
    x = lst[0]
    if x < 10:
        print(f'f: {x}')
        lst[0] += 1
        return x
    return None


def f0(x):
    sum = 0
    time.sleep(0.5)
    for i in range(x+1):
        sum += i
    return sum

def f1(x):
    time.sleep(0.5)
    return x+1

def f2(x):
    time.sleep(0.5)
    y = x*x
    # print(f'f2 {y}')
    return y
    
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
    # Complex example
    #        -- s1 --                                                             
    #       /        \                                                           
    # s0 --+--- s2 ---+-- s4                                                   
    #       \                                                                  
    #        -- s3 --N

    # s0 = Stage(func=f0, nprocs=3, order='ordered', inputs=True, outputs=True)
    # s1 = Stage(func=f1, nprocs=3, order='ordered', inputs=True, outputs=True)
    # s2 = Stage(func=f2, nprocs=3, order='ordered', inputs=True, outputs=True)
    # s3 = Stage(func=f3, nprocs=3, order='ordered', inputs=True, outputs=False)
    # s4 = Stage(func=f4, nprocs=3, order='ordered', inputs=True, outputs=True)

    # s0.link(s1, s2, s3)
    # s3.merge(s1, s2)


    # Simple example
    s0 = Stage(func=f0, nprocs=3, order='ordered', inputs=True, outputs=True)
    s1 = Stage(func=f1, nprocs=3, order='ordered', inputs=True, outputs=True)
    s2 = Stage(func=f2, nprocs=3, order='ordered', inputs=True, outputs=True)


    s0.link(s1.link(s2))
    # s0.link(s1)
    pipe = Pipeline(head_stage=s0, tail_stage=s2)
    pipe.start()

    # breakpoint()
    # Put values in pipe
    for i in range(10):
        print('input:', i)
        pipe.put(i)
    pipe.put(None)

    # breakpoint()
    # Get results from pipe
    while True:
        v = pipe.get()
        print('output:', v)
        if v is None:
            break

    


    
    # # How do I want to use the class

    # # 1. Define worker worker object
    # # w = MyWorkerClass(20)


    # # worker = MyWorkerClass(20)
    # # print(worker)
    # # p0 = GenVoidStage(worker)
    # # pipe = Pipeline(p0)
    # # pipe.start()
    

    # # # Test only one GenVoid pipline/stage

    # # worker = MyWorkerClass(20)
    # x = 0
    # worker = GenVoidWorker(f, args=([x,],))
    # p0 = stage(worker)
    # print(p0)
    # pipe = Pipeline(p0)
    # pipe.start()

    # print('Test 1 done.')

    # # # Test Geng -> Pipe -> Void pipeline
    # # gen = GenGen(10)
    # # p0 = GenWorker(target=gen.work, is_input=False)
    # # p1 = Stage(target=func, args=(2,))
    # # p2 = Stage(target=func, args=(2,))
    # # p3 = Stage(target=func, args=(2,))
    # # p4 = Stage(target=func, args=(2,))
    # # p5 = Stage(target=void_func, args=(3,), is_output=False)

    # # p0.link(p1)
    # # p1.link(p2)
    # # p2.link(p3)
    # # p3.link(p4)
    # # p4.link(p5)

    # # pipe = Pipeline(p0)
    # # pipe.start()
    # # print('Test 2 done.')

    # # Test Pipe -> Pipe -> Pipe pipeline
    # w1 = Square2()
    # w2 = Square()
    # w3 = Square()
    # w4 = Square()
    # print(w1)
    # p1 = stage(w1)
    # p2 = stage(w2)
    # p3 = stage(w3)
    # p4 = stage(w4)

    # p1.link(p2)
    # p2.link(p3)
    # p3.link(p4)

    # pipe = Pipeline(p1)
    # pipe.start()
    
    # # Put values in pipe
    # for i in range(10):
    #     print('input:', i)
    #     pipe.put(i)
    # pipe.put(None)

    # # Get results from pipe
    # while True:
    #     v = pipe.get()
    #     print('output:', v)
    #     if v is None:
    #         break

    # print('Test 3 done.')

    # # This is what split should look like
    # # p0.link(p1a.link(p1b.link(p1c)),
    # #         p2a.link(p2b.link(p2c)),
    # #         p3a.link(p3b.link(p3c)))

    # # This is what merge would look like
    # # p4.merge(p1c, p2c, p3c)
