try:
	import _py_hdfs
except ImportError:
	raise Exception("""Import Failed.  The following environmental variables should be set:

	LD_LIBRARY_PATH
	CLASSPATH

	See the example env.bash.
""")
from py_hdfs import HDFS

def connect(uri="default"):
        builder = _py_hdfs.NewBuilder()
        _py_hdfs.BuilderSetNameNode(builder, uri)
        return py_hdfs.HDFS(builder)
