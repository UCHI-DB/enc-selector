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

public class GlobalJoinFileProducer {

    public static void main(String[] args) throws IOException, VersionParser.VersionParseException {
        //args = new String[]{"DICT","DICT", "UNCOMPRESSED"};
        if (args.length == 0) {
            System.out.println("JoinFileProducer PPencoding LPencoding Compression");
            return;
        }
        String PPencoding = args[0];
        String LPencoding = args[1];
        String comp = args[2];
        String lineitem = "../tpch-generator/dbgen/lineitem";
        String part = "../tpch-generator/dbgen/part";
        URI[] input = {new File(lineitem+".tbl").toURI(), new File(part+".tbl").toURI()};
        int[] index = {1,0};
        MessageType[] schema ={TPCHSchema.lineitemSchema(), TPCHSchema.partSchema()};
        Boolean order = true;

        int intbound = ParquetWriterHelper.scanIntMaxInTab(new File(lineitem+".tbl").toURI(), 1);
        int bitLength = 32 - Integer.numberOfLeadingZeros(intbound);
        System.out.println("lineitem intBitLength: "+ bitLength +" lineitem intBound: "+intbound);

        EncContext.encoding.get().put(TPCHSchema.lineitemSchema().getColumns().get(1).toString(), IntEncoding.valueOf(LPencoding).parquetEncoding());
        EncContext.context.get().put(TPCHSchema.lineitemSchema().getColumns().get(1).toString(), new Integer[]{bitLength,intbound});
        Object2IntMap dictMap = ParquetWriterHelper.buildGlobalDict(input,index,schema,order);
        //System.out.println(dictMap);
        EncContext.globalDict.get().put(TPCHSchema.lineitemSchema().getColumns().get(1).toString(), dictMap);

        //System.out.println(Encoding.valueOf("PLAIN"));
        ParquetWriterHelper.write(new File(lineitem+".tbl").toURI(), TPCHSchema.lineitemSchema(),
                new File(lineitem+".parquet").toURI(), "\\|", false, comp);



        int pib = ParquetWriterHelper.scanIntMaxInTab(new File(part+".tbl").toURI(), 0);
        int pbl = 32 - Integer.numberOfLeadingZeros(intbound);
        System.out.println("part intBitLength: "+ bitLength +" part intBound: "+intbound);

        EncContext.encoding.get().put(TPCHSchema.partSchema().getColumns().get(0).toString(), IntEncoding.valueOf(PPencoding).parquetEncoding());
        EncContext.context.get().put(TPCHSchema.partSchema().getColumns().get(0).toString(), new Integer[]{pbl,pib});
        EncContext.globalDict.get().put(TPCHSchema.partSchema().getColumns().get(0).toString(), dictMap);

        ParquetWriterHelper.write(new File(part+".tbl").toURI(), TPCHSchema.partSchema(),
                new File(part+".parquet").toURI(), "\\|", false, comp);
        long pcolsize = ParquetReaderHelper.getColSize(new File(part+".parquet").toURI(), 0);
        //System.out.println("part col " + 0 + " size:"+pcolsize);

    }
}
