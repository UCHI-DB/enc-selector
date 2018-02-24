#! /bin/sh
# Chunwei Liu
# RLE BIT_PACKED 
BASEDIR=$(pwd)
for p_key in BP RLE PLAIN DICT DELTABP; do
	for l_key in BP RLE PLAIN DICT DELTABP; do
		echo "*********p_key: ${p_key} l_key: ${l_key}********"
		cd ~/enc-selector/
		java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.JoinFileProducer $p_key $l_key 
		java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.HashJoinTool
		#cd $BASEDIR
	done
done 

