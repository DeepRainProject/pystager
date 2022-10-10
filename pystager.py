from enum import Enum
from typing import Collection, Iterator, Collection
from time import perf_counter

from pathlib import Path
import shutil
import sys
import functools as ft
import itertools as it
from collections import deque

import logging
from concurrent.futures import ThreadPoolExecutor, Executor

from mpi4py.futures import MPIPoolExecutor # circumvents python gil, but requires objects to be picklable
from mpi4py.futures import MPICommExecutor # alternative (context manager)

# circumvent circular imports
from tasks import Mkdir, run_tasks
from pystager_data import HirarchicalFileTree, Node

GLOBAL_TIMING_MARKER = " TOTAL"

# setup logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

def filter_global_timings(record):
    i = record.msg.find(GLOBAL_TIMING_MARKER)
    if i == -1:
        return False
    
    record.msg = record.msg[:i]
    return True
           

class Pipeline:
    default_task = Mkdir()

    def __init__(
        self,
        source,
        dest,
        tasks, # Task | Collection[Task], requires python 3.10
        max_lvl,
        levels = None # : Collection[int] | None = None,requires python 3.10
    ):
        self.source = Path(source)
        self.dest = Path(dest)
        self.max_lvl = max_lvl
        self.tree = HirarchicalFileTree(Node(self.source, self.dest, self.max_lvl))

        self.tasks = (tasks if isinstance(tasks, list) else [tasks])

        self.levels = levels
        if self.levels is None:
            self.levels = [max_lvl for task in self.tasks] # default level => last
        
        for i in range(max_lvl): # assure at least default task per level
            if not i in self.levels:
                self.tasks.append(Pipeline.default_task)
                self.levels.append(i)

            
    def get_levels(self): # TODO: maybe dynamical max_depth
        tasks = [[] for _ in range(self.max_lvl+1)]
        for task, level in zip(self.tasks, self.levels):
            tasks[level].append(task)
        return zip(it.count(), self.tree, tasks)


class Backend(Enum):
    threading = ThreadPoolExecutor
    mpi = MPIPoolExecutor # TODO: make sure mpi is available on host machine


class Pystager:
    def __init__(self, max_workers=4, backend=Backend.threading.value, benchmark_file=None):
        self.max_workers = max_workers
        self.backend = backend
        # TODO: make sure #workers doesnt exceed available ressources
        self.exectuor: Executor = backend(max_workers=max_workers)

        # setup logging
        self.logger = logger.getChild(__class__.__name__)
        self.logger.setLevel(logging.INFO)

        syslog = logging.StreamHandler(sys.stdout)
        syslog.setFormatter(logging.Formatter("%(levelname)s %(name)s :: %(numprocs)s :: %(message)s"))
        self.logger.addHandler(syslog)

        if benchmark_file is not None:
            benchmarking = logging.FileHandler(benchmark_file)
            benchmarking.addFilter(filter_global_timings)
            benchmarking.setFormatter(logging.Formatter("%(numprocs)s; %(message)s"))
            self.logger.addHandler(benchmarking)

        extra = {"numprocs": self.max_workers}
        self.logger = logging.LoggerAdapter(self.logger, extra)

    
    def process(self, pipeline: Pipeline):
        """Process a Pipeline of tasks on the mapping Tree."""

        # get iterator over levels
        total_start = perf_counter()
        with self.backend(max_workers=self.max_workers) as executor:
            for level, nodes, tasks in pipeline.get_levels():
                args = [(tasks, node) for node in nodes]
                
                start = perf_counter()
                results = executor.map(run_tasks, args)
                success = all(results) # consume iterator
            
                self.logger.info(f"{round(perf_counter()-start,5)} {level}: Completeted tasks {[task.description for task in tasks]}")
        
        self.logger.info(f"{round(perf_counter()-total_start,5)}{GLOBAL_TIMING_MARKER}")
