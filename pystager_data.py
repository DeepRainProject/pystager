import itertools as it
from collections.abc import Iterable
from time import sleep

class Node:
    def __init__(self, source, dest, max_lvl,lvl=0):
        self.source = source
        self.dest = dest
        self.max_lvl = max_lvl
        self._children = None
        self.lvl = lvl
        self.deleted = False
        if self.lvl > self.max_lvl:
            raise Error("Hierarchical Tree is deeper then expected.")
    
    def instantiate_children(self):
        """Iterator that yields the children of the node."""

        try:
            subpaths = it.chain(*[src.iterdir() for src in self.source]) # if multiple sources
        except TypeError:
            subpaths = self.source.iterdir() # if only one source

        try:
            for relpath in (abspath.name for abspath in subpaths):
                node = Node(
                    self.source / relpath,
                    self.dest / relpath,
                    self.max_lvl,
                    lvl= self.lvl+1
                )
                yield node

        except NotADirectoryError:
            return None

    @property
    def children(self):
        if self._children is None:
            return list(self.instantiate_children()) # return fresh iterator
        
        return self._children

    @children.setter
    def children(self, children):
        self._children = list(children)

    def __str__(self):
        return f"lvl {self.lvl}: {self.source} -> {self.dest}"
    
    def __iter__(self):
        try:
            return iter(self.children)
        except TypeError:
            return iter([]) # if self.children is None => no children


class HirarchicalFileTree:
    def __init__(self, node):
        self.root_node = node
        self.lvl_queue = []

    def __iter__(self):
        return self    

    def __next__(self):
        if len(self.lvl_queue) == 0: # first lvl
            self.lvl_queue = [self.root_node]
        else: # all other lvls
            new_lvl = []
            for node in self.lvl_queue:
                if not node.deleted:
                    for child in node:
                        new_lvl.append(child)
            
            if len(new_lvl) == 0:
                raise StopIteration()

            self.lvl_queue = new_lvl
        return self.lvl_queue
 