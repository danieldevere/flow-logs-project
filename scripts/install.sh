#!/bin/bash

export PATH="$PATH:/usr/local/bin"

# git
sudo yum install -y git

# install thrift (jeez) https://thrift.apache.org/docs/install/centos
sudo yum -y update
sudo yum -y groupinstall "Development Tools"
wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz
tar xvf autoconf-2.69.tar.gz
cd autoconf-2.69
./configure --prefix=/usr
make
sudo make install
cd ..
sudo rm -rf autoconf*
wget http://ftp.gnu.org/gnu/automake/automake-1.14.tar.gz
tar xvf automake-1.14.tar.gz
cd automake-1.14
./configure --prefix=/usr
make
sudo make install
cd ..
sudo rm -rf automake*
wget http://ftp.gnu.org/gnu/bison/bison-2.5.1.tar.gz
tar xvf bison-2.5.1.tar.gz
cd bison-2.5.1
./configure --prefix=/usr
make
sudo make install
cd ..
sudo rm -rf bison*
wget https://www-us.apache.org/dist/thrift/0.11.0/thrift-0.11.0.tar.gz
tar xf thrift-0.11.0.tar.gz
cd thrift-0.11.0
./configure --with-lua=no
make
sudo make install
cd ..
sudo rm -rf thrift*

# Install my code

USR=$1
PSWD=$2

git clone "https://$USR:$PSWD@rev.cs.uchicago.edu/ddevere/ipaddressbyminute.git"
git clone "https://$USR:$PSWD@rev.cs.uchicago.edu/ddevere/hbase-rest.git"

cd ipaddressbyminute
./mvnw clean install -DskipTests
cp target/uber*.jar /home/hadoop/uber-jar.jar
cd ..
cd hbase-rest
./mvnw clean install -DskipTests
cp target/hbase-rest*.jar /home/hadoop/hbase-rest.jar
cd ..

git clone "https://$USR:$PSWD@rev.cs.uchicago.edu/ddevere/scripts.git"

hbase shell scripts/hbase.txt 

echo $(date +%s) > /home/hadoop/time.txt

# nohup java -jar /home/hadoop/hbase-rest.jar -Xmx500m &

# chmod 777 batch.sh 

# ./batch.sh "s3://dan-flow-logs/AWSLogs/" "dan-sequence-files"