#########################################################################
# File Name: run.sh
# Author: ma6174
# mail: ma6174@163.com
# Created Time: Fri 06 Mar 2015 11:31:53 PM EST
#########################################################################
#!/bin/bash
mkdir -p class/
javac -classpath hadoop-core-1.0.3.jar:/usr/local/hadoop/*.jar   *.java -d class

jar cvf run_hadoop_phrase.jar -C class/  .

#hadoop fs -rmr /user/lipingx/output/

#hadoop jar run_hadoop_phrase.jar run_hadoop_phrase  unigram_apple.txt bigram_apple.txt output/aggregated/ output/sizecount output/unigrammessage output/final
#rm -rf output/
#hadoop fs -get output .
