from libc.stdlib cimport malloc, free

#
# Some utility decls used in libhdfs.
#

cdef extern from "stdint.h":
    ctypedef int int32_t
    ctypedef long int int64_t
    ctypedef short int	int16_t
    ctypedef unsigned short int	uint16_t

cdef extern from "time.h":
    ctypedef long int time_t

#
# Some utility decls used in libhdfs.
#

cdef extern from "hdfs.h":
    struct hdfsBuilder

    ctypedef int32_t   tSize
    ctypedef time_t    tTime
    ctypedef int64_t   tOffset
    ctypedef uint16_t  tPort

    cdef enum _tObjectKind:
        kObjectKindFile = c'F'
        kObjectKindDirectory = c'D'
    ctypedef _tObjectKind tObjectKind

    struct hdfs_internal
    ctypedef hdfs_internal * hdfsFS

    struct hdfsFile_internal
    ctypedef hdfsFile_internal * hdfsFile    

    # Determine if a file is open for read.
    #
    # @param file     The HDFS file
    # @return         1 if the file is open for read; 0 otherwise
    #
    int hdfsFileIsOpenForRead(hdfsFile fname)

    #
    # Determine if a file is open for write.
    #
    # @param file     The HDFS file
    # @return         1 if the file is open for write; 0 otherwise
    #
    int hdfsFileIsOpenForWrite(hdfsFile file)

    # 
    # hdfsConnectAsUser - Connect to a hdfs file system as a specific user
    # Connect to the hdfs.
    # @param nn   The NameNode.  See hdfsBuilderSetNameNode for details.
    # @param port The port on which the server is listening.
    # @param user the user name (this is hadoop domain user). Or NULL is equivelant to hhdfsConnect(host, port)
    # @return Returns a handle to the filesystem or NULL on error.
    # @deprecated Use hdfsBuilderConnect instead. 
    #
    hdfsFS hdfsConnectAsUser(char* nn, tPort port, char *user)

    # 
    # hdfsConnect - Connect to a hdfs file system.
    # Connect to the hdfs.
    # @param nn   The NameNode.  See hdfsBuilderSetNameNode for details.
    # @param port The port on which the server is listening.
    # @return Returns a handle to the filesystem or NULL on error.
    # @deprecated Use hdfsBuilderConnect instead. 
    #
    hdfsFS hdfsConnect(char* nn, tPort port)

    # 
    # hdfsConnect - Connect to an hdfs file system.
    #
    # Forces a new instance to be created
    #
    # @param nn     The NameNode.  See hdfsBuilderSetNameNode for details.
    # @param port   The port on which the server is listening.
    # @param user   The user name to use when connecting
    # @return       Returns a handle to the filesystem or NULL on error.
    # @deprecated   Use hdfsBuilderConnect instead. 
    #
    hdfsFS hdfsConnectAsUserNewInstance(char* nn, tPort port, char *user )

    # 
    # hdfsConnect - Connect to an hdfs file system.
    #
    # Forces a new instance to be created
    #
    # @param nn     The NameNode.  See hdfsBuilderSetNameNode for details.
    # @param port   The port on which the server is listening.
    # @return       Returns a handle to the filesystem or NULL on error.
    # @deprecated   Use hdfsBuilderConnect instead. 
    #
    hdfsFS hdfsConnectNewInstance(char* nn, tPort port)

    # 
    # Connect to HDFS using the parameters defined by the builder.
    #
    # The HDFS builder will be freed, whether or not the connection was
    # successful.
    #
    # Every successful call to hdfsBuilderConnect should be matched with a call
    # to hdfsDisconnect, when the hdfsFS is no longer needed.
    #
    # @param bld    The HDFS builder
    # @return       Returns a handle to the filesystem, or NULL on error.
    #
    hdfsFS hdfsBuilderConnect(hdfsBuilder *bld)

    #
    # Create an HDFS builder.
    #
    # @return The HDFS builder, or NULL on error.
    #
    hdfsBuilder * hdfsNewBuilder()

    #
    # Force the builder to always create a new instance of the FileSystem,
    # rather than possibly finding one in the cache.
    #
    # @param bld The HDFS builder
    #
    void hdfsBuilderSetForceNewInstance(hdfsBuilder *bld)

    #
    # Set the HDFS NameNode to connect to.
    #
    # @param bld  The HDFS builder
    # @param nn   The NameNode to use.
    #
    #             If the string given is 'default', the default NameNode
    #             configuration will be used (from the XML configuration files)
    #
    #             If NULL is given, a LocalFileSystem will be created.
    #
    #             If the string starts with a protocol type such as file:// or
    #             hdfs://, this protocol type will be used.  If not, the
    #             hdfs:// protocol type will be used.
    #
    #             You may specify a NameNode port in the usual way by 
    #             passing a string of the format hdfs://<hostname>:<port>.
    #             Alternately, you may set the port with
    #             hdfsBuilderSetNameNodePort.  However, you must not pass the
    #             port in two different ways.
    #
    void hdfsBuilderSetNameNode(hdfsBuilder *bld, char *nn)

    #
    # Set the port of the HDFS NameNode to connect to.
    #
    # @param bld The HDFS builder
    # @param port The port.
    #
    void hdfsBuilderSetNameNodePort(hdfsBuilder *bld, tPort port)

    #
    # Set the username to use when connecting to the HDFS cluster.
    #
    # @param bld The HDFS builder
    # @param userName The user name.  The string will be shallow-copied.
    #
    void hdfsBuilderSetUserName(hdfsBuilder *bld, char *userName)

    #
    # Set the path to the Kerberos ticket cache to use when connecting to
    # the HDFS cluster.
    #
    # @param bld The HDFS builder
    # @param kerbTicketCachePath The Kerberos ticket cache path.  The string
    #                            will be shallow-copied.
    #
    void hdfsBuilderSetKerbTicketCachePath(hdfsBuilder *bld,
                                           char *kerbTicketCachePath)

    #
    # Free an HDFS builder.
    #
    # It is normally not necessary to call this function since
    # hdfsBuilderConnect frees the builder.
    #
    # @param bld The HDFS builder
    #
    void hdfsFreeBuilder(hdfsBuilder *bld)

    #
    # Set a configuration string for an HdfsBuilder.
    #
    # @param key      The key to set.
    # @param val      The value, or NULL to set no value.
    #                 This will be shallow-copied.  You are responsible for
    #                 ensuring that it remains valid until the builder is
    #                 freed.
    #
    # @return         0 on success; nonzero error code otherwise.
    #
    int hdfsBuilderConfSetStr(hdfsBuilder *bld, char *key,
                              char *val)

    #
    # Get a configuration string.
    #
    # @param key      The key to find
    # @param val      (out param) The value.  This will be set to NULL if the
    #                 key isn't found.  You must free this string with
    #                 hdfsConfStrFree.
    #
    # @return         0 on success; nonzero error code otherwise.
    #                 Failure to find the key is not an error.
    #
    int hdfsConfGetStr(char *key, char **val)

    #
    # Get a configuration integer.
    #
    # @param key      The key to find
    # @param val      (out param) The value.  This will NOT be changed if the
    #                 key isn't found.
    #
    # @return         0 on success; nonzero error code otherwise.
    #                 Failure to find the key is not an error.
    #
    int hdfsConfGetInt(char *key, int32_t *val)

    #
    # Free a configuration string found with hdfsConfGetStr. 
    #
    # @param val      A configuration string obtained from hdfsConfGetStr
    #
    void hdfsConfStrFree(char *val)

    # 
    # hdfsDisconnect - Disconnect from the hdfs file system.
    # Disconnect from hdfs.
    # @param fs The configured filesystem handle.
    # @return Returns 0 on success, -1 on error.
    #         Even if there is an error, the resources associated with the
    #         hdfsFS will be freed.
    #
    int hdfsDisconnect(hdfsFS fs)

    # 
    # hdfsOpenFile - Open a hdfs file in given mode.
    # @param fs The configured filesystem handle.
    # @param path The full path to the file.
    # @param flags - an | of bits/fcntl.h file flags - supported flags are O_RDONLY, O_WRONLY (meaning create or overwrite i.e., implies O_TRUNCAT), 
    # O_WRONLY|O_APPEND. Other flags are generally ignored other than (O_RDWR || (O_EXCL & O_CREAT)) which return NULL and set errno equal ENOTSUP.
    # @param bufferSize Size of buffer for read/write - pass 0 if you want
    # to use the default configured values.
    # @param replication Block replication - pass 0 if you want to use
    # the default configured values.
    # @param blocksize Size of block - pass 0 if you want to use the
    # default configured values.
    # @return Returns the handle to the open file or NULL on error.
    #
    hdfsFile hdfsOpenFile(hdfsFS fs, char* path, int flags,
                          int bufferSize, short replication, tSize blocksize)

    # 
    # hdfsCloseFile - Close an open file. 
    # @param fs The configured filesystem handle.
    # @param file The file handle.
    # @return Returns 0 on success, -1 on error.  
    #         On error, errno will be set appropriately.
    #         If the hdfs file was valid, the memory associated with it will
    #         be freed at the end of this call, even if there was an I/O
    #         error.
    #
    int hdfsCloseFile(hdfsFS fs, hdfsFile file)

    # 
    # hdfsExists - Checks if a given path exsits on the filesystem 
    # @param fs The configured filesystem handle.
    # @param path The path to look for
    # @return Returns 0 on success, -1 on error.  
    #
    int hdfsExists(hdfsFS fs, char *path)

    # 
    # hdfsSeek - Seek to given offset in file. 
    # This works only for files opened in read-only mode. 
    # @param fs The configured filesystem handle.
    # @param file The file handle.
    # @param desiredPos Offset into the file to seek into.
    # @return Returns 0 on success, -1 on error.  
    #
    int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos)

    # 
    # hdfsTell - Get the current offset in the file, in bytes.
    # @param fs The configured filesystem handle.
    # @param file The file handle.
    # @return Current offset, -1 on error.
    #
    tOffset hdfsTell(hdfsFS fs, hdfsFile file)

    # 
    # hdfsRead - Read data from an open file.
    # @param fs The configured filesystem handle.
    # @param file The file handle.
    # @param buffer The buffer to copy read bytes into.
    # @param length The length of the buffer.
    # @return      On success, a positive number indicating how many bytes
    #              were read.
    #              On end-of-file, 0.
    #              On error, -1.  Errno will be set to the error code.
    #              Just like the POSIX read function, hdfsRead will return -1
    #              and set errno to EINTR if data is temporarily unavailable,
    #              but we are not yet at the end of the file.
    #
    tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length)

    # 
    # hdfsPread - Positional read of data from an open file.
    # @param fs The configured filesystem handle.
    # @param file The file handle.
    # @param position Position from which to read
    # @param buffer The buffer to copy read bytes into.
    # @param length The length of the buffer.
    # @return      See hdfsRead
    #
    tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position,
                    void* buffer, tSize length)
    
    # 
    # hdfsWrite - Write data into an open file.
    # @param fs The configured filesystem handle.
    # @param file The file handle.
    # @param buffer The data.
    # @param length The no. of bytes to write. 
    # @return Returns the number of bytes written, -1 on error.
    #
    tSize hdfsWrite(hdfsFS fs, hdfsFile file, void* buffer,
                    tSize length)

    # 
    # hdfsWrite - Flush the data. 
    # @param fs The configured filesystem handle.
    # @param file The file handle.
    # @return Returns 0 on success, -1 on error. 
    #
    int hdfsFlush(hdfsFS fs, hdfsFile file)


    #
    # hdfsHFlush - Flush out the data in client's user buffer. After the
    # return of this call, new readers will see the data.
    # @param fs configured filesystem handle
    # @param file file handle
    # @return 0 on success, -1 on error and sets errno
    #
    int hdfsHFlush(hdfsFS fs, hdfsFile file)


    #
    # hdfsAvailable - Number of bytes that can be read from this
    # input stream without blocking.
    # @param fs The configured filesystem handle.
    # @param file The file handle.
    # @return Returns available bytes; -1 on error. 
    #
    int hdfsAvailable(hdfsFS fs, hdfsFile file)


    #
    # hdfsCopy - Copy file from one filesystem to another.
    # @param srcFS The handle to source filesystem.
    # @param src The path of source file. 
    # @param dstFS The handle to destination filesystem.
    # @param dst The path of destination file. 
    # @return Returns 0 on success, -1 on error. 
    #
    int hdfsCopy(hdfsFS srcFS, char* src, hdfsFS dstFS, char* dst)


    #
    # hdfsMove - Move file from one filesystem to another.
    # @param srcFS The handle to source filesystem.
    # @param src The path of source file. 
    # @param dstFS The handle to destination filesystem.
    # @param dst The path of destination file. 
    # @return Returns 0 on success, -1 on error. 
    #
    int hdfsMove(hdfsFS srcFS, char* src, hdfsFS dstFS, char* dst)


    #
    # hdfsDelete - Delete file. 
    # @param fs The configured filesystem handle.
    # @param path The path of the file. 
    # @param recursive if path is a directory and set to 
    # non-zero, the directory is deleted else throws an exception. In
    # case of a file the recursive argument is irrelevant.
    # @return Returns 0 on success, -1 on error. 
    #
    int hdfsDelete(hdfsFS fs, char* path, int recursive)

    #
    # hdfsRename - Rename file. 
    # @param fs The configured filesystem handle.
    # @param oldPath The path of the source file. 
    # @param newPath The path of the destination file. 
    # @return Returns 0 on success, -1 on error. 
    #
    int hdfsRename(hdfsFS fs, char* oldPath, char* newPath)


    # 
    # hdfsGetWorkingDirectory - Get the current working directory for
    # the given filesystem.
    # @param fs The configured filesystem handle.
    # @param buffer The user-buffer to copy path of cwd into. 
    # @param bufferSize The length of user-buffer.
    # @return Returns buffer, NULL on error.
    #
    char* hdfsGetWorkingDirectory(hdfsFS fs, char *buffer, size_t bufferSize)


    # 
    # hdfsSetWorkingDirectory - Set the working directory. All relative
    # paths will be resolved relative to it.
    # @param fs The configured filesystem handle.
    # @param path The path of the new 'cwd'. 
    # @return Returns 0 on success, -1 on error. 
    #
    int hdfsSetWorkingDirectory(hdfsFS fs, char* path)


    # 
    # hdfsCreateDirectory - Make the given file and all non-existent
    # parents into directories.
    # @param fs The configured filesystem handle.
    # @param path The path of the directory. 
    # @return Returns 0 on success, -1 on error. 
    #
    int hdfsCreateDirectory(hdfsFS fs, char* path)


    # 
    # hdfsSetReplication - Set the replication of the specified
    # file to the supplied value
    # @param fs The configured filesystem handle.
    # @param path The path of the file. 
    # @return Returns 0 on success, -1 on error. 
    #
    int hdfsSetReplication(hdfsFS fs, char* path, int16_t replication)

    # 
    # hdfsFileInfo - Information about a file/directory.
    #
    struct _hdfsFileInfo:
        tObjectKind mKind
        char *mName
        tTime mLastMod
        tOffset mSize
        short mReplication
        tOffset mBlockSize
        char *mOwner
        char *mGroup
        short mPermissions
        tTime mLastAccess
    ctypedef _hdfsFileInfo hdfsFileInfo

    # 
    # hdfsListDirectory - Get list of files/directories for a given
    # directory-path. hdfsFreeFileInfo should be called to deallocate memory. 
    # @param fs The configured filesystem handle.
    # @param path The path of the directory. 
    # @param numEntries Set to the number of files/directories in path.
    # @return Returns a dynamically-allocated array of hdfsFileInfo
    # objects; NULL on error.
    #
    hdfsFileInfo *hdfsListDirectory(hdfsFS fs, char* path, int *numEntries)

    # 
    # hdfsGetPathInfo - Get information about a path as a (dynamically
    # allocated) single hdfsFileInfo struct. hdfsFreeFileInfo should be
    # called when the pointer is no longer needed.
    # @param fs The configured filesystem handle.
    # @param path The path of the file. 
    # @return Returns a dynamically-allocated hdfsFileInfo object
    # NULL on error.
    #
    hdfsFileInfo *hdfsGetPathInfo(hdfsFS fs, char* path)


    # 
    # hdfsFreeFileInfo - Free up the hdfsFileInfo array (including fields) 
    # @param hdfsFileInfo The array of dynamically-allocated hdfsFileInfo
    # objects.
    # @param numEntries The size of the array.
    #
    void hdfsFreeFileInfo(hdfsFileInfo *hdfsFileInfo, int numEntries)


    # 
    # hdfsGetHosts - Get hostnames where a particular block (determined by
    # pos & blocksize) of a file is stored. The last element in the array
    # is NULL. Due to replication, a single block could be present on
    # multiple hosts.
    # @param fs The configured filesystem handle.
    # @param path The path of the file. 
    # @param start The start of the block.
    # @param length The length of the block.
    # @return Returns a dynamically-allocated 2-d array of blocks-hosts
    # NULL on error.
    #
    char*** hdfsGetHosts(hdfsFS fs, char* path, 
                         tOffset start, tOffset length)


    # 
    # hdfsFreeHosts - Free up the structure returned by hdfsGetHosts
    # @param hdfsFileInfo The array of dynamically-allocated hdfsFileInfo
    # objects.
    # @param numEntries The size of the array.
    #
    void hdfsFreeHosts(char ***blockHosts)


    # 
    # hdfsGetDefaultBlockSize - Get the default blocksize.
    #
    # @param fs            The configured filesystem handle.
    # @deprecated          Use hdfsGetDefaultBlockSizeAtPath instead.
    #
    # @return              Returns the default blocksize, or -1 on error.
    #
    tOffset hdfsGetDefaultBlockSize(hdfsFS fs)


    # 
    # hdfsGetDefaultBlockSizeAtPath - Get the default blocksize at the
    # filesystem indicated by a given path.
    #
    # @param fs            The configured filesystem handle.
    # @param path          The given path will be used to locate the actual
    #                      filesystem.  The full path does not have to exist.
    #
    # @return              Returns the default blocksize, or -1 on error.
    #
    tOffset hdfsGetDefaultBlockSizeAtPath(hdfsFS fs, char *path)


    # 
    # hdfsGetCapacity - Return the raw capacity of the filesystem.  
    # @param fs The configured filesystem handle.
    # @return Returns the raw-capacity; -1 on error. 
    #
    tOffset hdfsGetCapacity(hdfsFS fs)


    # 
    # hdfsGetUsed - Return the total raw size of all files in the filesystem.
    # @param fs The configured filesystem handle.
    # @return Returns the total-size; -1 on error. 
    #
    tOffset hdfsGetUsed(hdfsFS fs)

    # 
    # Change the user and/or group of a file or directory.
    #
    # @param fs            The configured filesystem handle.
    # @param path          the path to the file or directory
    # @param owner         User string.  Set to NULL for 'no change'
    # @param group         Group string.  Set to NULL for 'no change'
    # @return              0 on success else -1
    #
    int hdfsChown(hdfsFS fs, char* path, char *owner,
                  char *group)

    # 
    # hdfsChmod
    # @param fs The configured filesystem handle.
    # @param path the path to the file or directory
    # @param mode the bitmask to set it to
    # @return 0 on success else -1
    #
    int hdfsChmod(hdfsFS fs, char* path, short mode)

    # 
    # hdfsUtime
    # @param fs The configured filesystem handle.
    # @param path the path to the file or directory
    # @param mtime new modification time or -1 for no change
    # @param atime new access time or -1 for no change
    # @return 0 on success else -1
    #
    int hdfsUtime(hdfsFS fs, char* path, tTime mtime, tTime atime)

cdef class Python_hdfsBuilder:
    cdef hdfsBuilder * inner
    def __repr__(self):
        return '<struct hdfsBuilder>'

cdef class Python_hdfsFS:
    cdef hdfsFS inner
    def __repr__(self):
        return '<struct hdfsFS>'

cdef class Python_hdfsFile:
    cdef hdfsFile inner
    def __repr__(self):
        return '<struct hdfsFile>'

# int hdfsFileIsOpenForRead(hdfsFile fname)
def FileIsOpenForRead(Python_hdfsFile fname):
    return hdfsFileIsOpenForRead(fname.inner)

# int hdfsFileIsOpenForWrite(hdfsFile file)
def FileIsOpenForWrite(Python_hdfsFile fname):
    return hdfsFileIsOpenForWrite(fname.inner)

#hdfsFS hdfsConnectAsUser(char* nn, tPort port, char *user)
def ConnectAsUser(char* nn, tPort port, char *user):
    python_hdfsFS = Python_hdfsFS()
    python_hdfsFS.inner = hdfsConnectAsUser(nn, port, user)
    return python_hdfsFS

#hdfsFS hdfsConnect(char* nn, tPort port)
def Connect(char* nn, tPort port):
    python_hdfsFS = Python_hdfsFS()
    python_hdfsFS.inner = hdfsConnect(nn, port)
    return python_hdfsFS

# hdfsFS hdfsConnectAsUserNewInstance(char* nn, tPort port, char *user )
def ConnectAsUserNewInstance(char* nn, tPort port, char *user):
    python_hdfsFS = Python_hdfsFS()
    python_hdfsFS.inner = hdfsConnectAsUserNewInstance(nn, port, user)
    return python_hdfsFS

# hdfsFS hdfsConnectNewInstance(char* nn, tPort port)
def ConnectNewInstance(char* nn, tPort port):
    python_hdfsFS = Python_hdfsFS()
    python_hdfsFS.inner = hdfsConnectNewInstance(nn, port)
    return python_hdfsFS

# hdfsFS hdfsBuilderConnect(hdfsBuilder bld)
def BuilderConnect(Python_hdfsBuilder bld):
    python_hdfsFS = Python_hdfsFS()
    python_hdfsFS.inner = hdfsBuilderConnect(bld.inner)
    return python_hdfsFS

# hdfsBuilder *hdfsNewBuilder)
def NewBuilder():
    python_hdfsBuilder = Python_hdfsBuilder()
    python_hdfsBuilder.inner = hdfsNewBuilder()
    return python_hdfsBuilder

# void hdfsBuilderSetForceNewInstance(hdfsBuilder *bld)
def BuilderSetForceNewInstance(Python_hdfsBuilder bld):
    hdfsBuilderSetForceNewInstance(bld.inner)

# void hdfsBuilderSetNameNode(hdfsBuilder *bld, char *nn)
def BuilderSetNameNode(Python_hdfsBuilder bld, char *nn):
    hdfsBuilderSetNameNode(bld.inner, nn)

# void hdfsBuilderSetUserName(hdfsBuilder *bld, char *userName)
def BuilderSetUserName(Python_hdfsBuilder bld, char *userName):
    hdfsBuilderSetUserName(bld.inner, userName)

# void hdfsBuilderSetKerbTicketCachePath(hdfsBuilder *bld, char *kerbTicketCachePath)
def BuilderSetKerbTicketCachePath(Python_hdfsBuilder bld, char *kerbTicketCachePath):
    hdfsBuilderSetKerbTicketCachePath(bld.inner, kerbTicketCachePath)

# void hdfsFreeBuilder(hdfsBuilder *bld)
def FreeBuilder(Python_hdfsBuilder bld):
    hdfsFreeBuilder(bld.inner)

# int hdfsBuilderConfSetStr(hdfsBuilder *bld, char *key, char *val)
def BuilderConfSetStr(Python_hdfsBuilder bld, char *key, char *val):
    hdfsBuilderConfSetStr(bld.inner, key, val)

# FIXME:
# int hdfsConfGetStr(char *key, char **val)
# def ConfGetStr(char *key, char **val):
#     return hdfsConfGetStr(key, val)

# FIXME:
# int hdfsConfGetInt(char *key, int32_t *val)
# def ConfGetInt(char *key, int32_t *val):
#     return hdfsConfGetInt(key, val)

# void hdfsConfStrFree(char *val)
def ConfStrFree(char *val):
    hdfsConfStrFree(val)

# int hdfsDisconnect(hdfsFS fs)
def Disconnect(Python_hdfsFS fs):
    return hdfsDisconnect(fs.inner)

# hdfsFile hdfsOpenFile(hdfsFS fs, char* path, int flags,
# int bufferSize, short replication, tSize blocksize)
def OpenFile(Python_hdfsFS fs, char* path, int flags,
             int bufferSize, short replication, tSize blocksize):
    python_hdfsFile = Python_hdfsFile()
    python_hdfsFile.inner = hdfsOpenFile(fs.inner, path, flags,
                                         bufferSize, replication, blocksize)
    return python_hdfsFile

# int hdfsCloseFile(hdfsFS fs, hdfsFile file)
def CloseFile(Python_hdfsFS fs, Python_hdfsFile file):
    return hdfsCloseFile(fs.inner, file.inner)

# int hdfsExists(hdfsFS fs, char *path)
def Exists(Python_hdfsFS fs, char *path):
    return hdfsExists(fs.inner, path)

# int hdfsSeek(hdfsFS fs, hdfsFile file, tOffset desiredPos)
def Seek(Python_hdfsFS fs, Python_hdfsFile file, tOffset desiredPos):
    return hdfsSeek(fs.inner, file.inner, desiredPos)

# tOffset hdfsTell(hdfsFS fs, hdfsFile file)
def Tell(Python_hdfsFS fs, Python_hdfsFile file):
    return hdfsTell(fs.inner, file.inner)

# tSize hdfsRead(hdfsFS fs, hdfsFile file, void* buffer, tSize length)
def Read(Python_hdfsFS fs, Python_hdfsFile file, tSize length):
    cdef char * buf = <char *> malloc(length * sizeof(char))
    cdef tSize n = hdfsRead(fs.inner, file.inner, buf, length)
    py_buffer = [ buf[i] for i in range(length) ]
    free(buf)
    return (n, py_buffer)

# tSize hdfsPread(hdfsFS fs, hdfsFile file, tOffset position,
#                 void* buffer, tSize length)
def Pread(Python_hdfsFS fs, Python_hdfsFile file, tOffset position, tSize length):
    pass

# tSize hdfsWrite(hdfsFS fs, hdfsFile file, void* buffer,
#                 tSize length)
def Write(Python_hdfsFS fs, Python_hdfsFile file, object py_buffer):
    length = len(py_buffer)
    cdef char * buf = <char *> malloc(length * sizeof(char))
    for i in range(length):
        buf[i] = ord(py_buffer[i])
    cdef tSize n = <int> hdfsWrite(fs.inner, file.inner, buf, length)
    free(buf)
    return n

