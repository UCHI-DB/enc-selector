#! /bin/sh
# Chunwei Liu
BASEDIR=$(pwd)
for i in 1 5 10 15 20; do
	cd ~/tpch-generator/dbgen/
        ./dbgen -f -s $i
	echo "scale:$i"
	for col in 4; do
		for enc in BIT_PACKED PLAIN_DICTIONARY PLAIN RLE DELTA_BINARY_PACKED; do
			echo "$col $enc"
			cd ~/enc-selector/
			java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.ScanFileProducer $col $enc
			java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.Q6ScanTool
		done
	done

	for col in 5; do
                for enc in PLAIN_DICTIONARY PLAIN; do
                        echo "$col $enc"
                        cd ~/enc-selector/
                        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.ScanFileProducer $col $enc
                        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.Q6ScanTool
                done
        done
	

	for col in 6; do
                for enc in PLAIN_DICTIONARY PLAIN; do
                        echo "$col $enc"
                        cd ~/enc-selector/
                        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.ScanFileProducer $col $enc
                        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.Q6ScanTool
                done
        done

	for col in 10; do
                for enc in DELTA_LENGTH_BYTE_ARRAY DELTA_BYTE_ARRAY PLAIN_DICTIONARY PLAIN; do
                        echo "$col $enc"
                        cd ~/enc-selector/
                        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.ScanFileProducer $col $enc
                        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.Q6ScanTool
                done
        done

cd $BASEDIR
done
echo "query ended!" 
