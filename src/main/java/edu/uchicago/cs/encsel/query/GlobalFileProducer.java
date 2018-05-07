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

import edu.uchicago.cs.encsel.parquet.EncContext;
import edu.uchicago.cs.encsel.parquet.ParquetReaderHelper;
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper;
import edu.uchicago.cs.encsel.query.tpch.TPCHSchema;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntMap;

import java.io.File;
import java.io.IOException;

public class GlobalFileProducer {

    public static void main(String[] args) throws IOException, VersionParser.VersionParseException {
        //args = new String[]{"10","PLAIN_DICTIONARY", "UNCOMPRESSED","true"};
        if (args.length == 0) {
            System.out.println("ScanFileProducer pos enc compression order");
            return;
        }
        int col = Integer.parseInt(args[0]);
        String enc = args[1];
        String compre = args[2];
        String order = args[3];
        Boolean ordered = (order.equalsIgnoreCase("true") || order.equals("1"));

        String lineitem = "../tpch-generator/dbgen/lineitem";

        EncContext.encoding.get().put(TPCHSchema.lineitemSchema().getColumns().get(col).toString(), Encoding.valueOf(enc));
        EncContext.context.get().put(TPCHSchema.lineitemSchema().getColumns().get(col).toString(), new Integer[]{6,12});
        Object2IntMap dictMap = ParquetWriterHelper.buildGlobalDict(new File(lineitem+".tbl").toURI(),col,TPCHSchema.lineitemSchema(),ordered, 0);
        System.out.println(dictMap);
        EncContext.globalDict.get().put(TPCHSchema.lineitemSchema().getColumns().get(col).toString(), dictMap);

        ParquetWriterHelper.write(new File(lineitem+".tbl").toURI(), TPCHSchema.lineitemSchema(),
                new File(lineitem+".parquet").toURI(), "\\|", false, compre);
        long colsize = ParquetReaderHelper.getColSize(new File(lineitem+".parquet").toURI(), col);
        //System.out.println("col " + col + " size: "+colsize);
    }
}
