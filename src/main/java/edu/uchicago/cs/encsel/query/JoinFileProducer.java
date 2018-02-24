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

import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper;
import edu.uchicago.cs.encsel.query.tpch.TPCHSchema;
import org.apache.parquet.column.Encoding;


import java.io.File;
import java.io.IOException;


public class JoinFileProducer {

    public static void main(String[] args) throws IOException {
        //args = new String[]{"BP","BP"};
        if (args.length == 0) {
            System.out.println("JoinFileProducer PPencoding LPencoding");
            return;
        }
        String PPencoding = args[0];
        String LPencoding = args[1];

        EncContext.encoding.get().put(TPCHSchema.lineitemSchema().getColumns().get(1).toString(), IntEncoding.valueOf(LPencoding).parquetEncoding());
        EncContext.context.get().put(TPCHSchema.lineitemSchema().getColumns().get(1).toString(), new Integer[]{23,6000000});

        //System.out.println(Encoding.valueOf("PLAIN"));
        ParquetWriterHelper.write(new File("/home/cc/tpch-generator/dbgen/lineitem.tbl").toURI(), TPCHSchema.lineitemSchema(),
                new File("/home/cc/tpch-generator/dbgen/lineitem.parquet").toURI(), "\\|", false);


        EncContext.encoding.get().put(TPCHSchema.partSchema().getColumns().get(0).toString(), IntEncoding.valueOf(PPencoding).parquetEncoding());
        EncContext.context.get().put(TPCHSchema.partSchema().getColumns().get(0).toString(), new Integer[]{23,6000000});

        ParquetWriterHelper.write(new File("/home/cc/tpch-generator/dbgen/part.tbl").toURI(), TPCHSchema.partSchema(),
                new File("/home/cc/tpch-generator/dbgen/part.parquet").toURI(), "\\|", false);

    }
}



