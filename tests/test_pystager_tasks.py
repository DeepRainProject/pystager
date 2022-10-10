import ipdb
import pytest
from pathlib import Path
from filecmp import cmp
from shutil import rmtree
import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from tasks import Mkdir, Copy, CDOMap, GroupByName, create_task, Concat, Filter
from pystager_data import Node

TEST_DATA_ROOT = Path(__file__).parent / "data"



@pytest.fixture
def output_root():
    output = TEST_DATA_ROOT / "output"
    output.mkdir()
    yield output
    rmtree(output)


info_good_inner = {
    "descripiton": "Test Task for inner nodes",
    "on_leafs": False,
    "is_ordered": False,
    "max_workers": -1,
}
info_good_leaf = {
    "descripiton": "Test Task for leaf nodes",
    "on_leafs": True,
    "is_ordered": False,
    "max_workers": -1,
}
info_malformed = (
    {},
    {"description": "foo", "on_leafs": "Bar", "is_ordered": True, "max_workers": 0},
    {"description": 0, "on_leafs": 1, "max_workers": 2},
    {"foo": "Test Task", "bar": False, "baz": False, "bla": -1},
    {"Test Task", False, False, -1},
)

func_good = lambda node: node
func_malformed = (
    lambda node: None,
    lambda node, other: node,
    lambda node: (node, "other"),
    lambda: "other",
)


@pytest.fixture
def netcdf_node(output_root):
    return Node(
        TEST_DATA_ROOT / "input" / "test_node.nc",
        output_root / "test_node.nc",
        1,
        lvl=1,
    )


@pytest.fixture
def inner_node(output_root):
    return Node(
        TEST_DATA_ROOT / "input" / "groupby",
        output_root / "groupby",
        2,
        lvl=1,
    )

@pytest.fixture
def grouped_node(inner_node):
    tasks = [Mkdir(), GroupByName(".")]
    for task in tasks:
        inner_node = task.run(inner_node)
    return inner_node


def test_mkdir(inner_node):
    task = Mkdir()
    task.run(inner_node)
    assert inner_node.dest.is_dir()


def test_copy(netcdf_node):
    task = Copy()
    task.run(netcdf_node)
    assert netcdf_node.dest.is_file()
    assert cmp(netcdf_node.source, netcdf_node.dest)

def test_group_by(grouped_node):    
    for child, name in zip(grouped_node.children, ("x","y")): #TODO: investigate different behaviour (Enxhis machine fails and works for ("x","y"))
        assert child.dest.name == name
        
def test_concat(grouped_node):
    task = Concat()
    for child, name in zip(grouped_node.children, ("y","x")):
        task.run(child)
        
        with open(child.dest,"r") as out_file:
            lines = out_file.readlines()

            for file in child.source:
                with open(file, "r") as in_file:
                    assert in_file.readlines()[0] in lines[0]


def test_filter(inner_node):
    ipdb.set_trace()
    condition = lambda node: "x" in node.source.name
    task = Filter(condition)

    for child in inner_node.children:
        task.run(child)
        if condition(child):
            assert not child.deleted
        else:
            assert child.deleted


def test_cdomap(netcdf_node):
    assert True
    # task = CDOMap(grid_in, grid_out)  # TODO


@pytest.mark.parametrize("func", func_malformed)
def test_create_task_malformed_func(func, inner_node):
    with pytest.raises(Exception):
        task = create_task(func_malformed_1, info_good_inner)
        task.run(inner_node)


@pytest.mark.parametrize("info", info_malformed)
def test_create_task_malformed_info(info):
    with pytest.raises(Exception):
        create_task(func_good, info_malformed_1)
