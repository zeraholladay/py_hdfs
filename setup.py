from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

ext_modules=[
    Extension("_py_hdfs", ["py_hdfs/py_hdfs.pyx"], libraries=["/usr/lib/hdfs"]),
]

setup(
  name="py_hdfs",
  version='1.0',
  author='Zera Holladay',
  author_email='zeraholladay@gmail.com',
  url='http://localhost',
  packages=['py_hdfs'],
  cmdclass={"build_ext": build_ext},
  ext_modules=ext_modules
)
