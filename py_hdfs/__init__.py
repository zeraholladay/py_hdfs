import _py_hdfs
from py_hdfs import HDFS

def connect(uri="default"):
        builder = _py_hdfs.NewBuilder()
        _py_hdfs.BuilderSetNameNode(builder, uri)
        return py_hdfs.HDFS(builder)
