__version__="0.0.3"

import multiprocessing as mp
from dataclasses import dataclass
from typing import List, Callable, Union
import time
from tqdm import tqdm
from enum import Enum

class Signals(Enum):
    PROCESS_FINISHED=1
    SAMPLE_CONSUMED=2

class BaseProcess:
    def __init__(self, identifier):
        self.identifier = identifier

class StepConverter:
    def __init__(self, function, mp_wrapper=None):
        self.function = function
        self.mp_wrapper = mp_wrapper
     
    def __call__(self, *args, num_processes: int=1, size_queue: int=100):
        # create and return a step
        return Step(func=self.function, args=args, mp_wrapper=self.mp_wrapper, num_processes=num_processes, size_queue=size_queue)

class UnwrapList(list):
    pass

@dataclass
class Step:
    func: Callable
    args: List
    mp_wrapper: Callable
    num_processes: int = 1
    size_queue: int = 100

def producer_wrapper(init_func):
    
    def producer(q_out: mp.JoinableQueue, identifier, *args):
        
        func = init_func(*args, identifier=identifier)
        
        for json_object in func():
            q_out.put(json_object)
            
    return producer


class Consumer_Producer_wrapper:
    
    def __init__(self, init_func):
        self.init_func = init_func
    
    def __call__(self, q_in: mp.JoinableQueue, q_out: mp.JoinableQueue, identifier, *args):
        
        func = self.init_func(*args, identifier=identifier)
        
        def read_from_queue():
            while True:
                data = q_in.get()
                
                if data==Signals.PROCESS_FINISHED:
                    break
                
                yield data
                q_in.task_done()
                          
        for data in func(read_from_queue()):
            q_out.put(data)

def consumer_producer_wrapper(init_func):
    
    def consumer_producer(q_in: mp.JoinableQueue, q_out: mp.JoinableQueue, identifier, *args):
        func = init_func(*args, identifier=identifier)
        
        def read_from_queue():
            while True:
                data = q_in.get()
                
                if data==Signals.PROCESS_FINISHED:
                    break
                
                yield data
                q_in.task_done()
                          
        for data in func(read_from_queue()):
            q_out.put(data) 
            
    return consumer_producer


def consumer_wrapper(init_func):
    
    def consumer(q_in: mp.JoinableQueue, total_samples, identifier, *args):
        func = init_func(*args, identifier=identifier)
        
        def read_from_queue():
            while True:
                data = q_in.get()
                
                if data==Signals.PROCESS_FINISHED:
                    break
                
                yield data
                q_in.task_done()
                
        with tqdm(total=total_samples) as pbar:
            for data in func(read_from_queue()):
                pbar.update(1)
    return consumer

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
        mp.set_start_method('forkserver')
        self.manager = mp.Manager()
    
        self.queues = [self.manager.JoinableQueue(step.size_queue) for step in self.pipeline_steps[:-1]]
        
        # producer first
        first_step = self.pipeline_steps[0]
        self.pipe_line_processes = [[mp.Process(target=producer_wrapper(first_step.func), args=tuple([self.queues[0], identifier]+list(first_step.args)), daemon=True)  for identifier in range(first_step.num_processes)]]

        # producer consumer
        self.pipe_line_processes.extend([[mp.Process(target=consumer_producer_wrapper(step.func), args=tuple([self.queues[i_step],self.queues[i_step+1],identifier]+list(step.args)), daemon=True)  for identifier in range(step.num_processes)] for i_step,step in enumerate(self.pipeline_steps[1:-1]) ])
        
        # consumer last
        last_step = self.pipeline_steps[-1]
        self.pipe_line_processes.append([mp.Process(target=consumer_wrapper(last_step.func), args=tuple([self.queues[-1], self.total_samples,identifier]+list(last_step.args)), daemon=True)  for identifier in range(last_step.num_processes)])
        
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
                self.queues[i].put(Signals.PROCESS_FINISHED) # signal the end to all the next processes
                
        # w8 for the last processor
        for process in self.pipe_line_processes[-1]:
            process.join()