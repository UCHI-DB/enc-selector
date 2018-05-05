#! /bin/sh
# Chunwei Liu
# RLE BIT_PACKED 
BASEDIR=$(pwd)
java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.GlobalFileProducer 4 PLAIN_DICTIONARY UNCOMPRESSED true
for skip in true false; do
    for hard in true false; do
        cd ~/enc-selector/
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter true 1 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter true 2 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter true 4 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter true 8 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter true 16 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter true 32 $skip $hard
        #cd $BASEDIR
    done
done
java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.GlobalFileProducer 4 PLAIN_DICTIONARY UNCOMPRESSED false
for skip in true false; do
    for hard in true false; do
        cd ~/enc-selector/
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 1 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 2 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 4 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 8 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 16 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 32 $skip $hard
        #cd $BASEDIR
    done
done
java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.ScanFileProducer 4 PLAIN_DICTIONARY UNCOMPRESSED
for skip in true false; do
    for hard in true false; do
        cd ~/enc-selector/
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 1 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 2 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 4 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 8 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 16 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 32 $skip $hard
        #cd $BASEDIR
    done
done
java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.ScanFileProducer 4 PLAIN UNCOMPRESSED
for skip in true false; do
    for hard in false; do
        cd ~/enc-selector/
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 1 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 2 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 4 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 8 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 16 $skip $hard
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.query.IntEqualFilter false 32 $skip $hard
        #cd $BASEDIR
    done
done
echo "query ended!"