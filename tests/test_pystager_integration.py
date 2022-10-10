import pytest

from pathlib import Path
import os
import sys
from shutil import rmtree
import logging
from filecmp import dircmp

import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from pystager import Pipeline, Pystager, Backend
from tasks import Copy, Mkdir, GroupByName, Concat, Filter


TEST_DATA_ROOT = Path(__file__).parent / "data"
NTASKS = 3

@pytest.fixture
def output_root():
    output = TEST_DATA_ROOT / "output"
    output.mkdir()
    yield output
    rmtree(output)


def test_copy_small(output_root):
    out_tree = output_root / "foo"
    pipeline = Pipeline(
        TEST_DATA_ROOT / "input/foo",
        out_tree , Copy(), max_lvl=3)
    Pystager(max_workers=NTASKS, backend=Backend.mpi.value).process(pipeline)

    val_tree = TEST_DATA_ROOT / "validation/foo"
    print(list(val_tree.glob("*")), list(out_tree.glob("*")))
    assert all([
            out_path.relative_to(out_tree) == val_path.relative_to(val_tree)
            for (out_path, val_path) in 
            zip(out_tree.glob("**"), val_tree.glob("**"))
            ])

def test_filter_copy_small(output_root):
    out_tree = output_root / "foo"
    condition = lambda node: node.source.name == "b1"

    pipeline = Pipeline(
        TEST_DATA_ROOT / "input/foo",
        out_tree , [Filter(condition), Mkdir(), Copy()],levels=[2,2,3] ,max_lvl=3)
    Pystager(max_workers=NTASKS, backend=Backend.threading.value).process(pipeline)

    val_tree = TEST_DATA_ROOT / "validation/foo"
    print(list(val_tree.glob("*")), list(out_tree.glob("*")))
    assert all([
            out_path.relative_to(out_tree) == val_path.relative_to(val_tree)
            for (out_path, val_path) in 
            zip(out_tree.glob("**"), val_tree.glob("**"))
            ])

def test_mlairq(output_root):
    print(__name__)

    pipeline = Pipeline(
        TEST_DATA_ROOT / "mlairq_sample", 
        output_root / "mlairq_sample", 
        [Mkdir(), GroupByName("^......(..)",sorted=False), Concat()],
        1,
        levels=[0,0,1]
    )
    
    Pystager(max_workers=NTASKS, backend=Backend.threading.value).process(pipeline)

    print("finished mlairq test")

