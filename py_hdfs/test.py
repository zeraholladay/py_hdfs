from os import environ
import py_hdfs
from os import (O_RDONLY,
                O_WRONLY, 
                O_CREAT)

fname = "/dud.txt"

message = """
This is the message I'm writing.
Here is line two.
"""

fs = py_hdfs.Connect("default", 0)

# print py_hdfs.ListDirectory(fs, "/x")

py_hdfs.GetHosts(fs, "/dud.txt", 0, 0)

writeFile = py_hdfs.OpenFile(fs, fname, O_WRONLY | O_CREAT, 0, 0, 0)

n = py_hdfs.Write(fs, writeFile, message)

py_hdfs.CloseFile(fs, writeFile)

readFile = py_hdfs.OpenFile(fs, fname, O_RDONLY, 0, 0, 0)

n, array = py_hdfs.Read(fs, readFile, 4096)

print ''.join(map(chr, array))

py_hdfs.CloseFile(fs, readFile)
