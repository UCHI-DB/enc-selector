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
        	java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.tpch.LoadTPCH4Plain $comp
		java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.Q6ScanTool
        	echo "Best (DDPD)"
		java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.tpch.LoadTPCH4Best $comp
        	java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.Q6ScanTool
        	echo "Worst (PPDP)"
        	java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.tpch.LoadTPCH4Worst $comp
        	java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.Q6ScanTool
        	cd $BASEDIR
	done
done
echo "query ended!" 
