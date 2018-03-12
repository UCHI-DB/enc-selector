#! /bin/sh
# Chunwei Liu
# RLE BIT_PACKED 
BASEDIR=$(pwd)
for i in 1 5 10 15 20; do
    cd ~/tpch-generator/dbgen/
	./dbgen -f -s $i
    echo "scale:$i"
    for comp in
	p_key = BP RLE PLAIN DICT DELTABP
	l_key = BP RLE PLAIN DICT DELTABP
	echo "Best,${p_key},${l_key} part.0 lineitem.1"
	cd ~/enc-selector/
	java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.JoinFileProducer $p_key $l_key
	java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.HashJoinTool
	#cd $BASEDIR


done 
echo "query ended!"
