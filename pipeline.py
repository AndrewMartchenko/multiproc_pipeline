import multiprocessing as mp
# from stage import GenStage, VoidStage, PipeStage

class Pipeline:
    def __init__(self, head_stage, tail_stage, maxqsize=100):
        # Creat multiprocesses
        self.head_stage = head_stage
        self.tail_stage = tail_stage
        stage = head_stage
        # print(stage)
        self.mp_list = []
        while stage is not None:
            self.mp_list.append(mp.Process(target=stage.run))
            self.tail_stage = stage
            # if not isinstance(stage, (GenStage, PipeStage)):
                # break
            stage = stage.next_stage

        # if isinstance(self.head_stage, (VoidStage, PipeStage)):
        # Create input queue
        self.tx_q = mp.Queue(maxsize=maxqsize)
        self.head_stage.rx_q = self.tx_q
        # else:
            # self.tx_q = None
        
        # if isinstance(self.tail_stage, (GenStage, PipeStage)):
        # Create output queue
        self.rx_q = mp.Queue(maxsize=maxqsize)
        self.tail_stage.tx_q = self.rx_q
        # else:
            # self.rx_q = None

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

