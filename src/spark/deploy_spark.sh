if [ ! -d ~/jdk/jdk1.8.0_151 ]; then
    tar -zxf ~/jdk/jdk-8u151-linux-x64.tar.gz -C ~/jdk;
fi
if [ ! -d ~/scala/scala-2.12.4 ]; then
    tar -zxf ~/scala/scala-2.12.4.tgz -C ~/scala;
fi
if [ ! -d ~/spark/spark-2.2.1-bin-hadoop2.7 ]; then
    tar -zxf ~/spark/spark-2.2.1-bin-hadoop2.7.tgz -C ~/spark;
fi

JAVA_HOME=~/jdk/jdk1.8.0_151
JRE_HOME=${JAVA_HOME}/jre
CLASSPATH=${JAVA_HOME}/lib:${JRE_HOME}/lib
MASTER_HOST=10.60.45.11
SCALA_HOME=~/scala/scala-2.12.4
SPARK_HOME=~/spark/spark-2.2.1-bin-hadoop2.7

echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc
echo "export JRE_HOME=$JRE_HOME" >> ~/.bashrc
echo "export CLASSPATH=$CLASSPATH" >> ~/.bashrc
echo "export SCALA_HOME=$SCALA_HOME" >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$SCALA_HOME/bin:$PATH' >> ~/.bashrc

source ~/.bashrc

if [ ! -e "{SPARK_HOME}/conf/spark-env.sh" ]; then
    touch ${SPARK_HOME}/conf/spark-env.sh ;
fi

echo "export SPARK_MASTER_HOST=${MASTER_HOST}" >> \
        ${SPARK_HOME}/conf/spark-env.sh

${SPARK_HOME}/sbin/stop-slave.sh
${SPARK_HOME}/sbin/start-slave.sh spark://${MASTER_HOST}:7078
