#! /bin/sh
# Chunwei Liu
BASEDIR=$(pwd)
for i in 1 5 10 15 20 25 30 35 40; do
	cd ~/tpch-generator/dbgen/
        ./dbgen -f -s $i
        echo "scale:$i"
	cd ~/enc-selector/
        echo "Plain"
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.tpch.LoadTPCH4Plain
	    java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.Q6ScanTool
        echo "Best (DDPD)"
	    java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.tpch.LoadTPCH4Best
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.Q6ScanTool
        echo "Worst (PPDP)"
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.tpch.LoadTPCH4Worst
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.Q6ScanTool
        cd $BASEDIR
done
echo "query ended!" 
