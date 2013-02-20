export JAVA_HOME=$(readlink -f $(which java) | sed "s:/jre/bin/java::")
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:/usr/lib/hadoop/lib/native
export CLASSPATH=$(echo /etc/hadoop/conf $(find /usr/lib/hadoop* -type f -name "*.jar") | sed "s, ,:,g")
