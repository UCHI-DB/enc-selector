#! /bin/sh
# Chunwei Liu
BASEDIR=$(pwd)
for i in 1 10 30 50 70 90
do
	echo "$i"
	cd ~/tpch-generator/dbgen/
	./dbgen -f -s $i
	cd ~/enc-selector/
	java -cp target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.tpch.LoadTPCH4Plain 
	mvn surefire:test -Dtest=TPCHQueryTest
	java -cp target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.tpch.LoadTPCH4Default
	mvn surefire:test -Dtest=TPCHQueryTest
	cd $BASEDIR
done 
