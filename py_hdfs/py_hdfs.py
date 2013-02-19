import _py_hdfs
from os import (O_RDONLY,
                O_WRONLY, 
                O_CREAT)

class HDFS(object):
    def __init__(self, builder):
        """
        called from connect() not directly
        """
        self._hdfsFS = _py_hdfs.BuilderConnect(builder)
        if None == self._hdfsFS:
            raise Exception('')

    def disconnect(self):
        _py_hdfs.Disconnect(self._hdfsFS)

    def open(self, path, mode=O_RDONLY|O_WRONLY|O_CREAT):
        _hdfs_file = hdfs.OpenFile(self._hdfsFS, path, mode, 0, 0, 0)
        if _hdfs_file == None:
            raise Exception("")
        return HDFSFile(self._hdfsFS, _hdfs_file)

    def copy(self, src, dst):
        if not True == _py_hdfs.Copy(self._hdfsFS, src, 
                                     self._hdfsFS, dst):
            raise Exception("")

    def delete(self, path, recursive=False):
        r = 1 if recursive else 0
        if not True == _py_hdfs.Delete(self._hdfsFS, path, r):
            raise Exception("")

    def move(selfsrc, dst):
        if not True == _py_hdfs.Move(self._hdfsFS, src, 
                                     self._hdfsFS, dst):
            raise Exception("")

    def exists(self, path):
        return _py_hdfs.Exists(self._hdfsFS, path)

    def rename(self):
        if not True == _py_hdfs.Rename(self._hdfsFS, old_path, new_path):
            raise Exception("")

    def get_working_directory(self):
        path = _py_hdfs.GetWorkingDirectory(self._hdfsFS)
        if None == path:
            raise Exception("")
        return path

    def set_working_directory(self, path):
        path = _py_hdfs.SetWorkingDirectory(self._hdfsFS, path)
        if None == path:
            raise Exception("")
        return path

    def create_directory(self, path):
        path = _py_hdfs.CreateDirectory(self._hdfsFS, path)
        if None == path:
            raise Exception("")
        return path

    def list_directory(self, path="."):
        entries = _py_hdfs.ListDirectory(self._hdfsFS, path)
        if entries == None:
            raise Exception("")
        return entries

class HDFSFile(object):
    def __init__(self, _hdfsFS, _hdfs_file):
        self._hdfsFS = _hdfsFS
        self._hdfs_file = _hdfs_file

        return _py_hdfsFS.FileIsOpenForRead(self._hdfsFS,
                                            self._hdfs_file)

    def can_write(self):
        return _py_hdfsFS.FileIsOpenForRead(self._hdfsFS,
                                            self._hdfs_file)

    def close(self):
        _py_hdfsFS.CloseFile(self._hdfsFS,
                             self._hdfs_file)

    def read(self, length=1024):
        n, py_buf =_py_hdfs.Read(self._hdfsFS, self._hdfs_file, length)
        if -1 == n:
            raise Exception('')
        return ''.join(py_buf)

    def write(self, py_buf):
        length = len(py_buf)
        n = _py_hdfs.Write(self._hdfsFS, self._hdfs_file, py_buf)
        if n != length:
            raise Exception('')

    def flush(self):
        pass

    def hflush(self):
        pass
