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

import edu.uchicago.cs.encsel.parquet.EncReaderProcessor;
import edu.uchicago.cs.encsel.parquet.ParquetReaderHelper;
import edu.uchicago.cs.encsel.query.NonePrimitiveConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

public class TPCHLoadParquetFromHDFS {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.default.name", "hdfs://192.5.87.20:9000");
        configuration.set("dfs.client.use.datanode.hostname", "true");

        final int numThread = Integer.valueOf(args[1]);

        TPCHWorker worker;
        if (numThread == -1) {
            worker = new TPCHWorker(
                    configuration,
                    new EncReaderProcessor() {

                        @Override
                        public int expectNumThread() {
                            return 0;
                        }

                        @Override
                        public void processRowGroup(VersionParser.ParsedVersion version,
                                                    BlockMetaData meta, PageReadStore rowGroup) {

                        }
                    }, TPCHSchema.lineitemSchema());
        } else {
            worker = new TPCHWorker(configuration, new EncReaderProcessor() {

                @Override
                public int expectNumThread() {
                    return numThread;
                }

                @Override
                public void processRowGroup(VersionParser.ParsedVersion version,
                                            BlockMetaData meta, PageReadStore rowGroup) {
                    for (ColumnDescriptor cd : schema.getColumns()) {
                        ParquetReaderHelper.readColumn(schema.getType(cd.getPath()).asPrimitiveType(), cd,
                                rowGroup.getPageReader(cd), version, new NonePrimitiveConverter());
                    }
                }
            }, TPCHSchema.lineitemSchema());
        }
        worker.work(args[0]);
    }
}
