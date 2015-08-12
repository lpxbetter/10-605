#########################################################################
# File Name: run.sh
# Author: ma6174
# mail: ma6174@163.com
# Created Time: Tue Feb 10 23:24:05 2015
#########################################################################
#!/bin/bash
javac Aggregate.java
javac MessageGenerator.java
javac MessageUnigramCombiner.java

cat $1 | sort -k1 -T ./sorttemp | java -Xmx128m Aggregate 1 > bigram_processed.txt
cat $2 | sort -k1 -T ./sorttemp| java -Xmx128m Aggregate 0 > unigram_processed.txt

cat bigram_processed.txt | java -Xmx128m MessageGenerator > message.txt

cat message.txt unigram_processed.txt | sort -k1,1 -T ./sorttemp | java -Xmx128m MessageUnigramCombiner > message_unigram.txt

javac PhraseGenerator.java
cat message_unigram.txt bigram_processed.txt | sort -k1,2 -T ./sorttemp | java -Xmx128m PhraseGenerator

