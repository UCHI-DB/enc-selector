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
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */

package edu.uchicago.cs.encsel.query.tpch;

import edu.uchicago.cs.encsel.parquet.EncReaderProcessor;
import edu.uchicago.cs.encsel.parquet.ParquetReaderHelper;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.net.URI;

public class TPCHColumnMaterialize {

    public static void main(String[] args) throws Exception {

        URI file = new File(args[0]).toURI();
        int index = Integer.valueOf(args[1]);

        MessageType lineitemSchema = TPCHSchema.lineitemSchema();

        ColumnDescriptor cd = lineitemSchema.getColumns().get(index);

        ParquetReaderHelper.read(file, new EncReaderProcessor() {
            @Override
            public void processRowGroup(VersionParser.ParsedVersion version, BlockMetaData meta,
                                        PageReadStore rowGroup) {
                PageReader pageReader = rowGroup.getPageReader(cd);

            }
        });

    }
}
