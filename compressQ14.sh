#! /bin/sh
# Chunwei Liu
BASEDIR=$(pwd)
for i in 1 5 10 15 20 25 30 35 40; do
	cd ~/tpch-generator/dbgen/
        ./dbgen -f -s $i
        echo "scale:$i"
	for comp in UNCOMPRESSED SNAPPY GZIP LZO; do
		echo "compression:$comp"
		cd ~/enc-selector/
        echo "Plain"
        p_key = "PLAIN"
		l_key = "PLAIN"
		echo "${p_key},${l_key} part.0 lineitem.1"
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.JoinFileProducer $p_key $l_key $comp
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.HashJoinTool
        echo "Best (BP,BP)"
        p_key = "BP"
		l_key = "BP"
		echo "${p_key},${l_key} part.0 lineitem.1"
		java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.JoinFileProducer $p_key $l_key $comp
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.HashJoinTool
        echo "Worst (DICT,DICT)"
        p_key = "DICT"
		l_key = "DICT"
		echo "${p_key},${l_key} part.0 lineitem.1"
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.JoinFileProducer $p_key $l_key $comp
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.HashJoinTool
        cd $BASEDIR
	done
done
echo "query ended!" 
