import py_hdfs
from nose.tools import eq_, ok_, set_trace, nottest
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

def tearDown():
    py_hdfs.Disconnect(fs)

def mktemp():
    fname = str(uuid4())
    hdfs_file = py_hdfs.OpenFile(fs, fname, O_WRONLY | O_CREAT, 0, 0, 0)
    py_hdfs.CloseFile(fs, hdfs_file)
    return fname

def test_sane():
    ok_(fs)

def test_FileIsOpenForRead():
    fname = mktemp()
    hdfs_file = py_hdfs.OpenFile(fs, fname, O_RDONLY, 0, 0, 0)
    ok_(py_hdfs.FileIsOpenForRead(hdfs_file), 
        "File should be open for write.")
    py_hdfs.CloseFile(fs, hdfs_file)

def test_FileIsOpenForWrite():
    fname = mktemp()
    hdfs_file = py_hdfs.OpenFile(fs, fname, O_WRONLY, 0, 0, 0)
    ok_(py_hdfs.FileIsOpenForWrite(hdfs_file),
        "File should be open for write.")
    py_hdfs.CloseFile(fs, hdfs_file)

def test_NewBuilder():
    builder = py_hdfs.NewBuilder()
    ok_(builder.__class__.__name__ == 'Python_hdfsBuilder',
        "Builder type not expected.")

def test_BuilderSetForceNewInstance():
    pass

def test_BuilderSetNameNode():
    pass

def test_BuilderSetUserName():
    pass

def test_FreeBuilder():
    pass

def test_BuilderConfSetStr():
    pass

def test_ConfGetStr():
    pass

def test_ConfGetInt():
    pass

def test_ConfStrFree():
    pass

def test_Disconnect():
    pass

def test_OpenFile():
    fname = mktemp()
    ok_(py_hdfs.OpenFile(fs, fname, O_RDONLY, 0, 0, 0))

def test_CloseFile():
    fname = mktemp()
    hdfs_file = py_hdfs.OpenFile(fs, fname, O_RDONLY, 0, 0, 0)
    ok_(py_hdfs.CloseFile(fs, hdfs_file))

def test_Read():
    fname = mktemp()
    hdfs_file = py_hdfs.OpenFile(fs, fname, O_RDONLY, 0, 0, 0)
    n, _ = py_hdfs.Read(fs, hdfs_file, 1024)
    ok_(n > -1, "read should return > -1.")
    n, _ = py_hdfs.Read(fs, hdfs_file, 1024)
    ok_(n == 0, "read should return 0.")
    py_hdfs.CloseFile(fs, hdfs_file)

def test_Pread():
    fname = mktemp()
    hdfs_file = py_hdfs.OpenFile(fs, fname, O_RDONLY, 0, 0, 0)
    n, _ = py_hdfs.Pread(fs, hdfs_file, 0, 1024)
    ok_(n > -1, "read should return > -1.")

def test_Write():
    msg = """
An apple a day keeps
the doctor away.
"""
    fname = mktemp()
    hdfs_file = py_hdfs.OpenFile(fs, fname, O_WRONLY, 0, 0, 0)
    eq_(py_hdfs.Write(fs, hdfs_file, msg), len(msg))
    py_hdfs.CloseFile(fs, hdfs_file)

def test_Flush():
    fname = mktemp()
    hdfs_file = py_hdfs.OpenFile(fs, fname, O_WRONLY, 0, 0, 0)
    ok_(py_hdfs.Flush(fs, hdfs_file))
    py_hdfs.CloseFile(fs, hdfs_file)

def test_HFlush():
    fname = mktemp()
    hdfs_file = py_hdfs.OpenFile(fs, fname, O_WRONLY, 0, 0, 0)
    ok_(py_hdfs.HFlush(fs, hdfs_file))
    py_hdfs.CloseFile(fs, hdfs_file)

def test_Available():
    fname = mktemp()
    hdfs_file = py_hdfs.OpenFile(fs, fname, O_RDONLY, 0, 0, 0)
    ok_(py_hdfs.Available(fs, hdfs_file) != -1)
    py_hdfs.CloseFile(fs, hdfs_file)

def test_Copy():
    fname = mktemp()
    cp_fname = fname + '.copy'
    ok_(py_hdfs.Copy(fs, fname, fs, cp_fname))

def test_Delete():
    fname = mktemp()
    ok_(py_hdfs.Delete(fs, fname, 0))

def test_Move():
    fname = mktemp()
    mv_fname = fname + ".move"
    ok_(py_hdfs.Move(fs, fname, fs, mv_fname))

def test_Exists():
    ok_(py_hdfs.Exists(fs, "."))

def test_Rename():
    fname = mktemp()
    new_fname = fname + ".rename"
    ok_(py_hdfs.Rename(fs, fname, new_fname))

def test_GetWorkingDirectory():
    dir_name = py_hdfs.GetWorkingDirectory(fs)
    ok_(py_hdfs.SetWorkingDirectory(fs, dir_name))
    eq_(py_hdfs.GetWorkingDirectory(fs), dir_name)

def test_SetWorkingDirectory():
    """
    see test_GetWorkingDirectory
    """
    pass

def test_CreteDirectory():
    ok_(py_hdfs.CreateDirectory(fs, "."))

def test_SetReplication():
    fname = mktemp()
    ok_(py_hdfs.SetReplication(fs, fname, 2))

def test_ListDirectory():
    entry_list = py_hdfs.ListDirectory(fs, ".")
    for entry in entry_list:
        _ = entry.mKind
        _ = entry.mName
        _ = entry.mLastMod
        _ = entry.mSize
        _ = entry.mReplication
        _ = entry.mBlockSize
        _ = entry.mOwner
        _ = entry.mGroup
        _ = entry.mPermissions
        _ = entry.mLastAccess
    ok_(entry_list)

def test_GetPathInfo():
    entry = py_hdfs.GetPathInfo(fs, "/")
    _ = entry.mKind
    _ = entry.mName
    _ = entry.mLastMod
    _ = entry.mSize
    _ = entry.mReplication
    _ = entry.mBlockSize
    _ = entry.mOwner
    _ = entry.mGroup
    _ = entry.mPermissions
    _ = entry.mLastAccess
    ok_(entry)

def test_GetHosts():
    fname = mktemp()
    host_list = py_hdfs.GetHosts(fs, fname, 0, 0)
    ok_(len(host_list) == 0)
