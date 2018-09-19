#! /bin/sh
# Chunwei Liu
# RLE BIT_PACKED 
BASEDIR=$(pwd)
# filedir="../datasets/taxi/trip_data_2"
# schemadir="../datasets/taxi/trip_data"
filedir="../datasets/crime/crime"
schemadir="../datasets/crime/crime"
for intEnc in 0 1 2 3 4; do
    for stringEnc in 0 1 2 3; do
        cd ~/enc-selector/
        java -cp target/enc-selector-0.0.1-SNAPSHOT.jar:target/enc-selector-0.0.1-SNAPSHOT-jar-with-dependencies.jar edu.uchicago.cs.encsel.app.encoding.ParquetEncoder ${filedir}.csv ${schemadir}.schema ${filedir}_${intEnc}_${stringEnc}.parquet src/main/nnmodel/int_model/ src/main/nnmodel/string_model/ ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)" 1000000 ${intEnc} ${stringEnc}
    done
done
echo "Encoding ended!"