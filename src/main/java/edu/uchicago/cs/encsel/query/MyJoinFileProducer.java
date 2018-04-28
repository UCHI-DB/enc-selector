/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License,
 *
 * Contributors:
 *     Chunwei Liu - initial API and implementation
 *
 */

package edu.uchicago.cs.encsel.query;

import edu.uchicago.cs.encsel.model.IntEncoding;
import edu.uchicago.cs.encsel.parquet.EncContext;
import edu.uchicago.cs.encsel.parquet.ParquetReaderHelper;
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper;
import edu.uchicago.cs.encsel.query.tpch.TPCHSchema;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class MyJoinFileProducer {

    public static void main(String[] args) throws IOException, VersionParser.VersionParseException {
        args = new String[]{"PLAIN_DICTIONARY","PLAIN_DICTIONARY", "UNCOMPRESSED"};
        if (args.length == 0) {
            System.out.println("JoinFileProducer PPencoding LPencoding Compression");
            return;
        }
        String PPencoding = args[0];
        String LPencoding = args[1];
        String comp = args[2];
        String lineitem = "../tpch-generator/dbgen/lineitem";
        String orders = "../tpch-generator/dbgen/orders";
        URI[] input = {new File(lineitem+".tbl").toURI(), new File(orders+".tbl").toURI()};
        int[] index = {10,4};
        MessageType[] schema ={TPCHSchema.lineitemSchema(), TPCHSchema.orderSchema()};
        Boolean order = true;

        EncContext.encoding.get().put(TPCHSchema.lineitemSchema().getColumns().get(index[0]).toString(), Encoding.valueOf(PPencoding));
        EncContext.context.get().put(TPCHSchema.lineitemSchema().getColumns().get(index[0]).toString(), new Integer[]{1,2});
        Object2IntMap dictMap = ParquetWriterHelper.buildGlobalDict(input,index,schema,order);
        System.out.println(dictMap);
        EncContext.globalDict.get().put(TPCHSchema.lineitemSchema().getColumns().get(index[0]).toString(), dictMap);

        //System.out.println(Encoding.valueOf("PLAIN"));
        ParquetWriterHelper.write(new File(lineitem+".tbl").toURI(), TPCHSchema.lineitemSchema(),
                new File(lineitem+".parquet").toURI(), "\\|", false, comp);


        EncContext.encoding.get().put(TPCHSchema.orderSchema().getColumns().get(index[1]).toString(), Encoding.valueOf(LPencoding));
        EncContext.context.get().put(TPCHSchema.orderSchema().getColumns().get(index[1]).toString(), new Integer[]{1,2});
        EncContext.globalDict.get().put(TPCHSchema.orderSchema().getColumns().get(index[1]).toString(), dictMap);

        ParquetWriterHelper.write(new File(orders+".tbl").toURI(), TPCHSchema.orderSchema(),
                new File(orders+".parquet").toURI(), "\\|", false, comp);
        long pcolsize = ParquetReaderHelper.getColSize(new File(orders+".parquet").toURI(), index[1]);
        //System.out.println("orders col " + 0 + " size:"+pcolsize);

    }
}
