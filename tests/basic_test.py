from nose.tools import eq_, ok_, set_trace, nottest
import py_hdfs
from os import (O_RDONLY,
                O_WRONLY, 
                O_CREAT)
hdfs = None

def setUp():
    global hdfs
    hdfs = py_hdfs.connect()
    hdfs.mkdir("test")
    hdfs.cd("test")

def tearDown():
    for path in hdfs.ls():
        hdfs.delete(path, True)
    hdfs.disconnect()

def test_open_readonly():
    hdfs.touch("test.txt")
    with hdfs.open("test.txt") as f:
        ok_(f.can_read())

def test_open_write():
    hdfs.touch("test.txt")
    with hdfs.open("test.txt", O_WRONLY) as f:
        ok_(f.can_write())

def test_copy():
    hdfs.touch("file1.txt")
    hdfs.copy("file1.txt", "file2.txt")
    ok_(hdfs.exists("file1.txt"))
    ok_(hdfs.exists("file2.txt"))

def test_move():
    hdfs.touch("file1.txt")
    hdfs.move("file1.txt", "file2.txt")
    ok_(not hdfs.exists("file1.txt"))
    ok_(hdfs.exists("file2.txt"))

def test_rename():
    hdfs.touch("file1.txt")
    hdfs.rename("file1.txt", "file3.txt")
    ok_(not hdfs.exists("file1.txt"))
    ok_(hdfs.exists("file3.txt"))

def test_pwd():
    from os.path import basename
    eq_(basename(hdfs.pwd()), "test")

def test_ls():
    hdfs.touch("test.txt")
    ok_(len(hdfs.ls()) > 0)


