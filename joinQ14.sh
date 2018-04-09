#! /bin/sh
# Chunwei Liu
# RLE BIT_PACKED 
BASEDIR=$(pwd)
for i in 1 5 10 15 20; do
        cd ~/tpch-generator/dbgen/
        ./dbgen -f -s $i
        echo "scale:$i"
        for p_key in BP RLE PLAIN DICT DELTABP; do
                for l_key in BP RLE PLAIN DICT DELTABP; do
                        echo "${p_key},${l_key} part.0 lineitem.1"
                        cd ~/enc-selector/
                        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.JoinFileProducer $p_key $l_key UNCOMPRESSED
                        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.HashJoinTool
                        #cd $BASEDIR
                done
        done
done
echo "query ended!"