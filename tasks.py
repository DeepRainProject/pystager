from abc import ABC, abstractmethod
from time import sleep
import shutil
import sys
import logging
import subprocess
import itertools as it
import re
from pystager_data import Node

# setup logging
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)


class Task(ABC):
    def __init__(
        self, description: str, on_leafs: bool, is_ordered: bool, max_workers: int
    ):
        self.description: str = description
        self.on_leafs: bool = on_leafs
        self.is_ordered: bool = is_ordered  # allow arbitrary order of execution
        self.max_workers: int = (
            max_workers  # max num of workers (how much parallelization)
        )

    @abstractmethod
    def run(node):
        pass


class Copy(Task):
    def __init__(self):
        super().__init__("Copy Files", True, False, -1)

    def run(self, node):
        """copy files from location 'node.source' to 'node.dest' ."""
        logger.debug(f"copy files from {node.source} to {node.dest}")

        try:
            shutil.copyfile(node.source, node.dest)
        except Exception as e:
            logger.exception(f"could not copy file {node.dest}")
        return node


class Mkdir(Task):
    def __init__(self):
        super().__init__("Make Directory", False, False, -1)

    def run(self, node):
        logger.debug(f"make directory {node.dest}")
        try:
            node.dest.mkdir(exist_ok=True)
        except Exception as e:
            logger.exception(f"could not make directory {node.dest}")
        return node


class CDOMap(Task):
    def __init__(self, grid_in, grid_out):
        self.grid_in = grid_in
        self.grid_out = grid_out
        super().__init__("Remap data", True, False, -1)

    def run(self, node):
        logger.debug(f"remapping data")
        try:
            subprocess.run(
                [
                    "cdo",
                    "-L",
                    "-z",
                    "zip_6",
                    f"remapcon,{self.grid_out}",
                    f"-setgrid,{self.grid_in}",
                    "-selvar,yw_hourly",
                    node.source,
                    node.dest,
                ]
            )
        except Exception as e:
            logger.exception(f"Exception occured:\n{e}")
        return node


class GroupByName(Task):
    def __init__(self, regex, sorted=True):
        """Definine regex pattern for extracting common new name"""
        self.regex = regex
        self.sorted = sorted
        super().__init__("Group by name", False, False, -1)

    
    def run(self, node):
        """group children together by name."""

        children = node.children
        if not self.sorted:
            children = sorted(children, key=self.keyfunc)

        node.children = [
            Node([child.source for child in child_group], node.dest / key, max_lvl=node.max_lvl, lvl=node.lvl+1)
            for key, child_group in it.groupby(children, self.keyfunc)
        ]

        for child in node.children:
            logger.debug(f"grouping {len(child.source)} nodes together to path {child.dest}")

        return node

    def keyfunc(self, child):
        try:
            return re.match(self.regex, child.source.name).group(0)
        except:
            logger.warning("pattern for groupby doesnt match, using identity as keyfunc")
            return child.source.name


class Concat(Task):
    def __init__(self):
        super().__init__("Concat files", True, False, -1)

    def run(self, node):
        """Process node wich conatains list of grouped files concat them into single file."""

        with open(node.dest, "w") as out_file:
            for file in node.source:
                with open(file) as in_file:
                    out_file.writelines(in_file.readlines())
        
        return node


class Filter_Years(Task):
    def __init__(self, args):
        self.args = args
        super().__init__("Filter years - 1st level nodes", False, False, -1)

    def run(self, node):
        if self.condition(node):
            return node
        else:
            node.deleted = True
            return node

    def condition(self, node):
        if self.args.year_start <= int(node.source.name) <= self.args.year_end:
            return True
        else:
            return False


class Filter_Months(Task):
    def __init__(self, args):
        self.args = args
        super().__init__("Filter months - 2nd level nodes", False, False, -1)

    def run(self, node):
        if self.condition(node):
            return node
        else:
            node.deleted = True
            return node

    def condition(self, node):
        if self.args.year_start == self.args.year_end:
            if self.args.month_start <= int(node.source.name[-2:]) <= self.args.month_end:
                return True
        elif int(node.source.parts[-2])==self.args.year_start:
            if self.args.month_start <= int(node.source.name[-2:]):
                return True
        elif int(node.source.parts[-2])==self.args.year_end:
            if int(node.source.name[-2:]) <= self.args.month_end:
                return True
        elif self.args.year_start < int(node.source.parts[-2]) < self.args.year_end:
            return True
        else:
            return False


def create_task(func, info):
    """wrap a function into a (non-parameterizable) task."""

    class Inner(Task):
        def __init__(self):
            super().__init__(**info)

        def run(self, node):
            result = func()
            if result is None:
                raise Exception(
                    "a Task should always return the potentially modified node"
                )
            return result

    return Inner()

def run_tasks(args):
    tasks, node = args
    for task in tasks:
        node = task.run(node)
        if node.deleted:
            break
    
    return True

def time_task(func):
    def inner(node):
        logger.info(f"{round(perf_counter()-start,5)} {level}: {task.description}")
        return func(node)
    return inner
