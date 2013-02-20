import _py_hdfs
from urlparse import urlparse
from os import (O_RDONLY,
                O_WRONLY, 
                O_CREAT)

class HDFS_Exception(Exception):
    pass

class HDFS(object):
    def __init__(self, builder):
        """
        called from connect() not directly
        """
        self._hdfsFS = _py_hdfs.BuilderConnect(builder)
        if None == self._hdfsFS:
            raise HDFS_Exception("HDFS connection failed.")

    def __enter__(self):
        return self

    def __exit__(self,type, value, traceback):
        self.disconnect()

    def disconnect(self):
        _py_hdfs.Disconnect(self._hdfsFS)

    def open(self, path, mode=O_RDONLY):
        """
        Mode may be:
        O_RDONLY
        O_WRONLY
        O_WRONLY|O_APPEND
        """
        _hdfs_file = _py_hdfs.OpenFile(self._hdfsFS, path, mode, 0, 0, 0)
        if _hdfs_file == None:
            raise HDFS_Exception("HDFS open failed: %s" % path)
        else:
            return HDFSFile(self._hdfsFS, _hdfs_file)

    def touch(self, path):
        with self.open(path, O_WRONLY):
            pass

    def copy(self, src, dst):
        if not True == _py_hdfs.Copy(self._hdfsFS, src, 
                                     self._hdfsFS, dst):
            raise HDFS_Exception("HDFS copy failed: %s to %s" % (src, dst))

    def delete(self, path, recursive=False):
        r = 1 if recursive else 0
        if not True == _py_hdfs.Delete(self._hdfsFS, path, r):
            raise HDFS_Exception("HDFS delete failed:" % (path))

    def move(self, src, dst):
        if not True == _py_hdfs.Move(self._hdfsFS, src, 
                                     self._hdfsFS, dst):
            raise HDFS_Exception("HDFS move failed: %s to %s" % (src, dst))

    def exists(self, path):
        return _py_hdfs.Exists(self._hdfsFS, path)

    def rename(self, old_path, new_path):
        if not _py_hdfs.Rename(self._hdfsFS, old_path, new_path):
            raise HDFS_Exception("HDFS rename failed: %s to %s" % (old_path, new_path))

    def pwd(self):
        return urlparse(self.get_working_directory()).path

    def get_working_directory(self):
        path = _py_hdfs.GetWorkingDirectory(self._hdfsFS)
        if None == path:
            raise HDFS_Exception("HDFS get_working_directory failed")
        else:
            return path

    def cd(self, path):
        if self.set_working_directory(path):
            return self.pwd()
        else:
            raise HDFS_Excption("HDFS failed to cd to path: %s" % path)

    def set_working_directory(self, path):
        boolean = _py_hdfs.SetWorkingDirectory(self._hdfsFS, path)
        if None == boolean:
            raise HDFS_Exception("")
        else:
            return path

    def mkdir(self, path):
        return self.create_directory(path)

    def create_directory(self, path):
        path = _py_hdfs.CreateDirectory(self._hdfsFS, path)
        if None == path:
            raise HDFS_Exception("HDFS create_directory failed")
        else:
            return path

    def ls(self, path="."):
        return [ urlparse(entry.mName).path for entry in
                 self.list_directory(path) ]

    def list_directory(self, path="."):
        return _py_hdfs.ListDirectory(self._hdfsFS, path)

class HDFSFile(object):
    def __init__(self, _hdfsFS, _hdfs_file):
        self._hdfsFS = _hdfsFS
        self._hdfs_file = _hdfs_file

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def can_read(self):
        return _py_hdfs.FileIsOpenForRead(self._hdfs_file)

    def can_write(self):
        return _py_hdfs.FileIsOpenForWrite(self._hdfs_file)

    def close(self):
        _py_hdfs.CloseFile(self._hdfsFS, self._hdfs_file)

    def read_str(self, length=1024):
        return "".join(map(chr, self.read(length)))

    def read(self, length=1024):
        """
        Probably want read_str.
        """
        n, py_buf =_py_hdfs.Read(self._hdfsFS, self._hdfs_file, length)
        if -1 == n:
            raise HDFS_Exception("HDFS read failed")
        else:
            return py_buf

    def pread(self, position, length):
        pass

    def write(self, py_buf):
        length = len(py_buf)
        n = _py_hdfs.Write(self._hdfsFS, self._hdfs_file, py_buf)
        if n != length:
            raise HDFS_Exception("HDFS write failed")
        else:
            return self

    def flush(self):
        boolean = _py_hdfs.Flush(self._hdfsFS, self._hdfs_file)
        if not boolean:
            raise HDFS_Exception("HDFS flush failed.")
        else:
            return self

    def hflush(self):
        boolean = _py_hdfs.HFlush(self._hdfsFS, self._hdfs_file)
        if not boolean:
            raise HDFS_Exception("HDFS hflush failed.")
        else:
            return self
