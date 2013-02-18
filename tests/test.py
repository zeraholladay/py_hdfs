import py_hdfs
from nose.tools import eq_, ok_, set_trace, with_setup, nottest
from uuid import uuid4
from os import (O_RDONLY,
                O_WRONLY, 
                O_CREAT)
fs = None

def setUp():
    global fs
    builder = py_hdfs.NewBuilder()
    py_hdfs.BuilderSetNameNode(builder, "default")
    fs = py_hdfs.BuilderConnect(builder)

def disconnect():
    py_hdfs.Disconnect(fs)

def test_sane():
    ok_(fs)

def test_Exists():
    eq_(py_hdfs.Exists(fs, "."), True)
    eq_(py_hdfs.Exists(fs, ""), False)

def test_CreteDirectory():
    eq_(py_hdfs.CreateDirectory(fs, "."), True)
    eq_(py_hdfs.CreateDirectory(fs, ""), False)

def test_GetWorkingDirectory():
    eq_(py_hdfs.GetWorkingDirectory(fs), "/")

# class Test(object):
#     def random_fname(self):
#         return "%s/%s.test" % ( self.test_dir, str(uuid4()) )

#     def setUp(self):
#         self.test_dir = "/%s" % str(uuid4())
#         self.hdfsFS = hdfs.Connect("default", 0)
#         hdfs.CreateDirectory(self.hdfsFS, self.test_dir)

#     def tearDown(self):
#         hdfs.Delete(self.hdfsFS, self.test_dir, 1)
#         hdfs.Disconnect(self.hdfsFS)

#     def touch(self, fname):
#         hdfs_file = hdfs.OpenFile(self.hdfsFS, fname, O_WRONLY | O_CREAT, 0, 0, 0)
#         hdfs.CloseFile(self.hdfsFS, hdfs_file)

#     def rm(self, fname):
#         hdfs_file = hdfs.OpenFile(self.hdfsFS, fname, O_CREAT, 0, 0, 0)
#         hdfs.Delete(hdfs_file, 0)
#         hdfs.CloseFile(self.hdfsFS, hdfs_file)

#     def test_ListDirectory(self):
#         fname = self.random_fname()
#         self.touch(fname)
        
#         entry_list = hdfs.ListDirectory(self.hdfsFS, self.test_dir)
#         for entry in entry_list:
#             _ = entry.mKind
#             _ = entry.mName
#             _ = entry.mLastMod
#             _ = entry.mSize
#             _ = entry.mReplication
#             _ = entry.mBlockSize
#             _ = entry.mOwner
#             _ = entry.mGroup
#             _ = entry.mPermissions
#             _ = entry.mLastAccess
#         ok_(entry_list)

#     def test_GetPathInfo(self):
#         entry = hdfs.GetPathInfo(self.hdfsFS, "/")
#         _ = entry.mKind
#         _ = entry.mName
#         _ = entry.mLastMod
#         _ = entry.mSize
#         _ = entry.mReplication
#         _ = entry.mBlockSize
#         _ = entry.mOwner
#         _ = entry.mGroup
#         _ = entry.mPermissions
#         _ = entry.mLastAccess
#         ok_(entry)

#     # def test_GetHosts(self):
#     #     fname = "%s/%s.test" % ( self.test_dir, str(uuid4()) )
#     #     self.touch(fname)
#     #     host_list = hdfs.GetHosts(self.hdfsFS, fname, 0, 0)
#     #     self.rm(fname)
#     #     ok_(len(host_list) > 0)
