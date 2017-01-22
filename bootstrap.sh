#!/bin/bash

: ${HADOOP_PREFIX:=/usr/local/hadoop}

$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid

# installing libraries if any - (resource urls added comma separated to the ACP system variable)
cd $HADOOP_PREFIX/share/hadoop/common ; for cp in ${ACP//,/ }; do  echo == $cp; curl -LO $cp ; done; cd -

# altering the core-site configuration
sed s/HOSTNAME/$HOSTNAME/ /usr/local/hadoop/etc/hadoop/core-site.xml.template > /usr/local/hadoop/etc/hadoop/core-site.xml

cp /etc/hosts ~/hosts.temp
sed -i "s/127.0.0.1\tlocalhost/127.0.0.1\tlocalhost $HOSTNAME/" ~/hosts.temp
echo "172.17.0.2 master" >> ~/hosts.temp
cp -f ~/hosts.temp /etc/hosts

sed -i "s/$HOSTNAME:9000/master:54310/" /usr/local/hadoop/etc/hadoop/core-site.xml

sed -i "s/<value>1<\/value>/<value>2<\/value>/" /usr/local/hadoop/etc/hadoop/hdfs-site.xml

rm -Rf /tmp/hadoop-root/*

if [[ $2 == "-master" ]]; then
  #echo "ello this is a mssg
  echo "master" > /usr/local/hadoop/etc/hadoop/masters
  echo "master" > /usr/local/hadoop/etc/hadoop/slaves
  export MAX_IP=$(($3+1))
  echo "MAX_IP $MAX_IP"
  cp /etc/hosts ~/hosts.temp
  for i in $(seq 3 $MAX_IP)
  	do
  		export SLAVE_NUM=$(($i-1))
  		echo "SLAVE_NUM $SLAVE_NUM"
  		echo "172.17.0.$i slave$SLAVE_NUM" >> ~/hosts.temp
  		echo "slave$SLAVE_NUM" >> /usr/local/hadoop/etc/hadoop/slaves
  	done
  #cp /etc/hosts ~/hosts.new
  #sed -i "s/\t$HOSTNAME/\t$HOSTNAME master/" ~/hosts.new
  cp -f ~/hosts.temp /etc/hosts
  #echo "Port 54310" >> /etc/ssh/sshd_config
  #service ssh start
  hadoop namenode -format
  #hadoop fs -mkdir -p /user/root
  #hadoop fs -put /Volumes/data/*.dat /user/root
  /spark-2.0.2/sbin/start-master.sh
  /zeppelin-0.6.2-bin-all/bin/zeppelin-daemon.sh start
  /spark-2.0.2/bin/pyspark --packages com.databricks:spark-csv_2.10:1.1.0 --master spark://172.17.0.2:7077 --executor-memory 512M --driver-memory 512M &
fi

if [[ $2 == "-slave" ]]; then
  cp /etc/hosts ~/hosts.new
  sed -i "s/\t$HOSTNAME/\t$HOSTNAME slave2/" ~/hosts.new
  cp -f ~/hosts.new /etc/hosts
  #service ssh start
  /spark-2.0.2/sbin/start-slave.sh 172.17.0.2:7077
  echo "slave2" > /usr/local/hadoop/etc/hadoop/slaves  #Hard-coded for now

fi

service ssh start
$HADOOP_PREFIX/sbin/start-dfs.sh
#$HADOOP_PREFIX/sbin/start-yarn.sh

if [[ $2 == "-master" ]]; then
  hadoop dfsadmin -safemode leave
	hadoop fs -mkdir -p /user/root
  hadoop fs -put /Volumes/data/*.dat /user/root
fi
#hadoop fs -mkdir /user/root
#hadoop fs -put /Volumes/data/*.dat /user/root

if [[ $1 == "-d" ]]; then
  while true; do sleep 1000; done
fi

if [[ $1 == "-bash" ]]; then
  /bin/bash
fi
