from os import environ
import py_hdfs
from os import O_WRONLY, O_CREAT

#fs = py_hdfs.Connect("hdfs://ip-10-40-229-80.ec2.internal", 8020)
fs = py_hdfs.Connect("default", 0)

writeFile = py_hdfs.OpenFile(fs, "/dud.txt", O_WRONLY | O_CREAT, 0, 0, 0)

n = py_hdfs.Write(fs, writeFile, "this is dud.txt changed")

print n, len("this is dud.txt")

py_hdfs.CloseFile(fs, writeFile)
