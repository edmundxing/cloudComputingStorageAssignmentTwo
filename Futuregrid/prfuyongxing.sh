# Fuyong Xing
# Department of Electrical and Computer Engineering
# University of Florida
# This code is for Programming Assignment 2 at the course: Cloud Computing and Storage.
# It has been submitted to the Futuregrid server: india. 
# It will run the file PageRankFuyongXing.jar, which is generated from
# the same source code file PageRankFuyong.java (but with different file name) submitted in this folder.

#!/bin/bash

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
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -mkdir DataFYX
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyFromLocal ${HOME}/DataFuyong2 DataFYX
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFYX/DataFuyong2

# PageRank on small size data
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar ${HOME}/PageRankFuyongXing.jar PageRankFuyongXing DataFYX/DataFuyong2/smallfiles 10
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFYX/DataFuyong2/smallfilesSortedResults
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFYX/DataFuyong2/smallfilesSortedResults ${HOME}/smallfilesSortedResultsFYX
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFYX/DataFuyong2/smallfilesStatResults
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFYX/DataFuyong2/smallfilesStatResults ${HOME}/smallfilesStatResultsFYX
# PageRank on medium size data
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar ${HOME}/PageRankFuyongXing.jar PageRankFuyongXing DataFYX/DataFuyong2/mediumfiles 10
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFYX/DataFuyong2/mediumfilesSortedResults
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFYX/DataFuyong2/mediumfilesSortedResults ${HOME}/mediumfilesSortedResultsFYX
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFYX/DataFuyong2/mediumfilesStatResults
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFYX/DataFuyong2/mediumfilesStatResults ${HOME}/mediumfilesStatResultsFYX
# PageRank on large size data
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR jar ${HOME}/PageRankFuyongXing.jar PageRankFuyongXing DataFYX/DataFuyong2/largefiles 10
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFYX/DataFuyong2/largefilesSortedResults
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFYX/DataFuyong2/largefilesSortedResults ${HOME}/largefilesSortedResultsFYX
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -ls DataFYX/DataFuyong2/largefilesStatResults
$HADOOP_HOME/bin/hadoop --config $HADOOP_CONF_DIR dfs -copyToLocal DataFYX/DataFuyong2/largefilesStatResults ${HOME}/largefilesStatResultsFYX

#### Stop the Hadoop cluster
echo "Stop all Hadoop daemons"
$HADOOP_HOME/bin/stop-all.sh
echo

#### Clean up the working directories after job completion
echo "Clean up"
$MY_HADOOP_HOME/bin/pbs-cleanup.sh -n 4 -c $HADOOP_CONF_DIR
echo
