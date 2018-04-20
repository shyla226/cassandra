
# The directory where Cassandra's configs live (required)
CASSANDRA_CONF=/etc/cassandra

CASSANDRA_HOME=/usr/share/cassandra

# the default location for commitlogs, sstables, and saved caches
# if not set in cassandra.yaml
cassandra_storagedir=/var/lib/cassandra

# The java classpath (required)
if [ -n "$CLASSPATH" ]; then
    CLASSPATH=$CLASSPATH:$CASSANDRA_CONF
else
    CLASSPATH=$CASSANDRA_CONF
fi

for jar in /usr/share/cassandra/lib/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

for jar in /usr/share/cassandra/*.jar; do
    CLASSPATH=$CLASSPATH:$jar
done

CLASSPATH="$CLASSPATH:$EXTRA_CLASSPATH"


# set JVM javaagent opts to avoid warnings/errors
if [ "$JVM_VENDOR" != "OpenJDK" -o "$JVM_VERSION" \> "1.6.0" ] \
      || [ "$JVM_VERSION" = "1.6.0" -a "$JVM_PATCH_VERSION" -ge 23 ]
then
    JAVA_AGENT="$JAVA_AGENT -javaagent:$CASSANDRA_HOME/lib/jamm-0.3.0.jar"
fi

tempdir_base="${TMPDIR-/tmp}/dse"
mkdir -p "${tempdir_base}"
instance_tempdir="$(mktemp -p ${tempdir_base} -d -t dse.XXXXXXXXXX)"
mkdir -p "${instance_tempdir}"
if [ ! -d "${instance_tempdir}" ] ; then
    echo "DSE instance temporary directory could not be created (${instance_tempdir} in ${tempdir_base})" > /dev/stderr
    exit 1
fi
mkdir -p "${instance_tempdir}/lni"
export TMPDIR="${instance_tempdir}"
JVM_OPTS="$JVM_OPTS -Djava.io.tmpdir=${instance_tempdir} -Djdk.internal.lambda.dumpProxyClasses=${instance_tempdir}/lni"
