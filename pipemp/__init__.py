version="0.0.1"

import multiprocessing as mp
from dataclasses import dataclass
from typing import List, Callable
import time
from tqdm import tqdm


@dataclass
class Step:
    func: Callable
    args: List
    num_processes: int = 1
    size_queue: int = 100


class Pipeline:
    def __init__(self, pipeline_steps: List[Step], total_samples=None):
        """
        Pipeline([
            Step(), Step(), Step()
        ])
        """
        assert len(pipeline_steps)>2
        self.pipeline_steps = pipeline_steps
        self.total_samples = total_samples
        self.manager = mp.Manager()
    
        self.queues = [self.manager.JoinableQueue(step.size_queue) for step in self.pipeline_steps[:-1]]
        
        # producer first
        first_step = self.pipeline_steps[0]
        self.pipe_line_processes = [[mp.Process(target=producer_wrapper(first_step.func), args=tuple([self.queues[0]]+first_step.args+[f"Producer #{identifier}"]), daemon=True)  for identifier in range(first_step.num_processes)]]

        # producer consumer
        self.pipe_line_processes.extend([[mp.Process(target=consumer_producer_wrapper(step.func), args=tuple([self.queues[i_step],self.queues[i_step+1]]+step.args+[f"Consumer-Producer L{i_step+1}#{identifier}"]), daemon=True)  for identifier in range(step.num_processes)] for i_step,step in enumerate(self.pipeline_steps[1:-1]) ])
        
        # consumer last
        last_step = self.pipeline_steps[-1]
        self.pipe_line_processes.append([mp.Process(target=consumer_wrapper(last_step.func), args=tuple([self.queues[-1], self.total_samples]+last_step.args+[f"Consumer #{identifier}"]), daemon=True)  for identifier in range(last_step.num_processes)])
        
    def run(self, debug_inspect_queue_sizes=False):
        
        for process_list in self.pipe_line_processes:
            for process in process_list:
                process.start()

        if debug_inspect_queue_sizes:
            while True:
                out_str = "Queue sizes | "
                for i,queue in enumerate(self.queues):
                    out_str += f"Queue #{i}: {queue.qsize()} | "
                print(out_str)
                time.sleep(0.5)

        # join
        for i in range(len(self.queues)):
            for process in self.pipe_line_processes[i]:
                process.join()
            
            self.queues[i].join()
            
            for _ in self.pipe_line_processes[i+1]:
                self.queues[i].put(None) # signal the end to all the next processes
                
        # w8 for the last processor
        for process in self.pipe_line_processes[-1]:
            process.join()
        
def producer_wrapper(func):
    
    def producer(q_out: mp.JoinableQueue, *args):
        
        lazy_func = func(*args)
        
        for json_object in lazy_func:
            q_out.put(json_object)
            
    return producer

def consumer_producer_wrapper(func):
    
    def consumer_producer(q_in: mp.JoinableQueue, q_out: mp.JoinableQueue, *args):
        
        lazy_func = func(*args)
        
        while True:
        
            data = q_in.get()
            
            if data==None:
                break
            
            out_data = lazy_func(data)
            
            q_out.put(out_data)
            q_in.task_done()
            
    return consumer_producer

def consumer_wrapper(func):
    
    def consumer(q_in: mp.JoinableQueue, total_samples, *args):
        
        lazy_func = func(*args)
        with tqdm(total=total_samples) as pbar:
            while True:
            
                data = q_in.get()
                
                if data==None:
                    break
                
                lazy_func(data)
                q_in.task_done()
                pbar.update(1)
            
    return consumer