from nose.tools import eq_, ok_, set_trace, nottest
import py_hdfs

hdfs = None

def setUp():
    global hdfs
    hdfs = py_hdfs.connect()

def tearDown():
    hdfs.disconnect()

def test_test():
    ok_(True)


