#!/bin/bash

#PBS -q batch
#PBS -N hadoop_job
#PBS -l nodes=4:ppn=8
#PBS -o hadoop_run.out
#PBS -e hadoop_run.err
#PBS -V

#module load myhadoop
module add java

### Run the myHadoop environment script to set the appropriate variables
#
# Note: ensure that the variables are set correctly in bin/setenv.sh
. $MY_HADOOP_HOME/bin/setenv.sh

#### Set this to the directory where Hadoop configs should be generated
# Don't change the name of this variable (HADOOP_CONF_DIR) as it is
# required by Hadoop - all config files will be picked up from here
#
# Make sure that this is accessible to all nodes
export HADOOP_CONF_DIR="${HOME}/myHadoop-config"
export HADOOP_HOME="/opt/hadoop-0.20.2"
#export MY_HADOOP_HOME="/opt/myHadoop"

#### Set up the configuration
# Make sure number of nodes is the same as what you have requested from PBS
# usage: $MY_HADOOP_HOME/bin/pbs-configure.sh -h
echo "Set up the configurations for myHadoop"
# this is the non-persistent mode
$MY_HADOOP_HOME/bin/pbs-configure.sh -n 4 -c $HADOOP_CONF_DIR
# this is the persistent mode
# $MY_HADOOP_HOME/bin/pbs-configure.sh -n 4 -c $HADOOP_CONF_DIR -p -d /oasis/cloudstor-group/HDFS
echo

#### Format HDFS, if this is the first time or not a persistent instance
echo "Format HDFS"
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR namenode -format
echo

#### Start the Hadoop cluster
echo "Start all Hadoop daemons"
$HADOOP_HOME/bin/start-all.sh
#$HADOOP_HOME/bin/hadoop dfsadmin -safemode leave
echo

### Run jobs
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -mkdir DataFXH
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyFromLocal ${HOME}/DataFuyong/smallfile DataFXH
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFXH/smallfile
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyFromLocal ${HOME}/DataFuyong/mediumfile DataFXH
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFXH/mediumfile
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyFromLocal ${HOME}/DataFuyong/largefile DataFXH
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFXH/largefile

# PageRank on small size data
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar ${HOME}/PageRankFuyong.jar PageRankFuyong DataFXH/smallfile 50
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFXH/smallfile22
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFXH/smallfile22 ${HOME}/PRoutputs
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFXH/smallfileSortedResults
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFXH/smallfileSortedResults ${HOME}/PRoutputs
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFXH/smallfileStatResults
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFXH/smallfileStatResults ${HOME}/PRoutputs
# PageRank on medium size data
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar ${HOME}/PageRankFuyong.jar PageRankFuyong DataFXH/mediumfile 50
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFXH/mediumfile11
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFXH/mediumfile11 ${HOME}/PRoutputs
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFXH/mediumfileSortedResults
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFXH/mediumfileSortedResults ${HOME}/PRoutputs
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFXH/mediumfileStatResults
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFXH/mediumfileStatResults ${HOME}/PRoutputs
# PageRank on large size data
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar ${HOME}/PageRankFuyong.jar PageRankFuyong DataFXH/largefile 50
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFXH/largefile37
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFXH/largefile37 ${HOME}/PRoutputs
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFXH/largefileSortedResults
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFXH/largefileSortedResults ${HOME}/PRoutputs
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFXH/largefileStatResults
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFXH/largefileStatResults ${HOME}/PRoutputs

#### Stop the Hadoop cluster
echo "Stop all Hadoop daemons"
$HADOOP_HOME/bin/stop-all.sh
echo

#### Clean up the working directories after job completion
echo "Clean up"
$MY_HADOOP_HOME/bin/pbs-cleanup.sh -n 4 -c $HADOOP_CONF_DIR
echo
