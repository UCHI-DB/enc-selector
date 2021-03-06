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
 *     Hao Jiang - initial API and implementation
 *
 */

package edu.uchicago.cs.encsel.query.tpch;

import edu.uchicago.cs.encsel.adapter.parquet.EncReaderProcessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class TPCHLoadParquetInMem {

    static final CompressionCodecName[] codecs =
            {CompressionCodecName.UNCOMPRESSED, CompressionCodecName.LZO, CompressionCodecName.GZIP};

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.set("fs.default.name", "hdfs://192.5.87.20:9000");
        configuration.set("dfs.client.use.datanode.hostname", "true");

        new TPCHWorker(configuration, new EncReaderProcessor() {
            @Override
            public int expectNumThread() {
                return 0;
            }

            @Override
            public void processRowGroup(VersionParser.ParsedVersion version, BlockMetaData meta, PageReadStore rowGroup) {

            }
        }, TPCHSchema.lineitemSchema()).work(args[0]);
    }


}
