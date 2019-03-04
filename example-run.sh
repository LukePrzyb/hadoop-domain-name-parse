#!/bin/bash

# A simple bash script for running tests with the input files provided
# on the github repository
# By: Luke (luke.a.programmer@gmail.com)
# Last Updated: March 1st, 2019
#
# This script assumes you are running a basic setup based on the
# Apache Hadoop tutorial site: https://hadoop.apache.org
# The Tutorial: http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html
# Tested on Apache Hadoop 3.2.0


HADOOP_DIR="."
DICTIONARY_FILE="dictionary.txt"

while getopts h:d flags
do
	case "${flags}"
	in
	h) HADOOP_DIR=${OPTARG};;
	d) DICTIONARY_FILE=${OPTARG};;
	esac
done

# Check for HADOOP_CLASSPATH which is crucial for java to compile and run on hadoop
if [ -z ${HADOOP_CLASSPATH} ]
then 
	# /usr/lib/jvm/java-1.8.0/lib/tools.jar
	echo ""
	echo "the environment variable HADOOP_CLASSPATH is not set. This may cause the system to fail compiling and running the program."
	echo ""
fi

PROJECT_DIR=`pwd`

CD_CHECK="$(cd $HADOOP_DIR 2>&1)"

if [ "$CD_CHECK" ]
then
	echo "invalid hadoop directory provided. Provide a valid directory relative to where this script is."
	echo "$CD_CHECK"
	exit 1
fi

cd $HADOOP_DIR

HADOOP_WORKING_DIR=`pwd`

# Generate random directory name for folder to run tests on
TEMP_FOLDER=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1)

$(cp $PROJECT_DIR/DomainParse.java .)

# COMPILE_CHECK="$(bin/hadoop com.sun.tools.javac.Main -cp $PROJECT_DIR/DomainParse.java -d $PROJECT_DIR 2>&1)"
# COMPILE_CHECK="$(bin/hadoop com.sun.tools.javac.Main $PROJECT_DIR/DomainParse.java 2>&1)"
COMPILE_CHECK="$(bin/hadoop com.sun.tools.javac.Main DomainParse.java 2>&1)"

if [ "$COMPILE_CHECK" ]
then
	echo "Error while compiling DomainParse"
	echo "$COMPILE_CHECK"
	exit 1
fi

$(jar -cf wc.jar DomainParse*.class)

# exit 0

$(bin/hadoop fs -mkdir /tmp$TEMP_FOLDER) && echo "Made temporary hadoop directory."

echo "Compiled the java program. Running Tests."
echo ""

TEST_COUNT=$(find $PROJECT_DIR/tests/* -name "test*" -type d | wc -l)

# Loop through our three tests
# for i in {1..3}
# for i in $(seq 0 $TEST_COUNT)
for ((i=0;i<$TEST_COUNT;i++))
do
	echo "Running Test $i"

	$(bin/hadoop fs -mkdir /tmp$TEMP_FOLDER/input_test_$i) && echo "Made temporary input directory" &&
	$(bin/hdfs dfs -put $PROJECT_DIR/tests/test$i/input* /tmp$TEMP_FOLDER/input_test_$i) && echo "Added our test file into hdfs"

	# stderr for hadoop is actually debugging information, while stdout is any system prints that happen in our java program.
	ERROR_CHECK=$(bin/hadoop jar wc.jar DomainParse /tmp$TEMP_FOLDER/input_test_$i /tmp$TEMP_FOLDER/output_$i $PROJECT_DIR/$DICTIONARY_FILE 2>$PROJECT_DIR/tests/test$i/hadoop_debug$i.txt)

	if ! bin/hdfs dfs -test -e /tmp$TEMP_FOLDER/output_$i/_SUCCESS
	then
		echo "Test $i failed."
		echo "Hadoop info was written in $PROJECT_DIR/tests/test$i/hadoop_debug$i.txt"
		echo "Domain Parser response: $ERROR_CHECK"
		$(bin/hadoop fs -rm -r /tmp$TEMP_FOLDER)
		exit 1
	fi

	echo "Storing test results in the test directory."

	# Place our results into our test directory.
	$(bin/hdfs dfs -get /tmp$TEMP_FOLDER/output_$i/part-r-* $PROJECT_DIR/tests/test$i/output_result.txt)
	(sort -o $PROJECT_DIR/tests/test$i/output_result.txt $PROJECT_DIR/tests/test$i/output_result.txt)
	(sort -o $PROJECT_DIR/tests/test$i/output_expected.txt $PROJECT_DIR/tests/test$i/output_expected.txt)

	echo ""
done

echo "Finished Tests."
$(bin/hadoop fs -rm -r /tmp$TEMP_FOLDER)

exit 0