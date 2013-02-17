from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

ext_modules=[
    Extension("py_hdfs", ["py_hdfs.pyx"], libraries=["/usr/lib/hdfs"]),
]

setup(
  name = "py_hdfs",
  cmdclass = {"build_ext": build_ext},
  ext_modules = ext_modules
)
