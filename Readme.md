# py_hdfs

A Beta Cython wrapper around libhdfs.  Theres still a lot to do.

## Installation

Requirements:

    Cython==0.15.1
    nose==1.1.2

Install:

    $ python setup.py install

## Usage

Setup the environment first:

    $ export JAVA_HOME=$(readlink -f $(which java) | sed "s:/jre/bin/java::")
    $ export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:/usr/lib/hadoop/lib/native
    $ export CLASSPATH=$(echo /etc/hadoop/conf $(find /usr/lib/hadoop* -type f -name "*.jar") | sed "s, ,:,g")

Test:

    $ python setup.py build_ext --inplace
    $ nosetests

Code:

    >>> import py_hdfs
    >>> hdfs = py_hdfs.connect() # default connection
    >>> dir(hdfs)
    ...
    >>> hdfs.ls()
    >>> hdfs.pwd()

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
