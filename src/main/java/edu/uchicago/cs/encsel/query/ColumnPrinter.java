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

package edu.uchicago.cs.encsel.query;

import edu.uchicago.cs.encsel.adapter.parquet.EncReaderProcessor;
import edu.uchicago.cs.encsel.adapter.parquet.ParquetReaderHelper;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

import java.net.URI;

/**
 * Utilities class to load a parquet file and print it out
 */
public class ColumnPrinter {


    public static void main(String[] args) throws Exception {
        String file = args[0];
        int columnIndex = Integer.valueOf(args[1]);

        ParquetReaderHelper.read(new URI(file), new EncReaderProcessor() {

            @Override
            public int expectNumThread() {
                return 0;
            }

            @Override
            public void processRowGroup(VersionParser.ParsedVersion version, BlockMetaData meta, PageReadStore rowGroup) {
                ColumnDescriptor descriptor = this.schema.getColumns().get(columnIndex);
                ColumnReaderImpl reader = new ColumnReaderImpl(descriptor, rowGroup.getPageReader(descriptor),
                        new PrintConverter(schema.getType(columnIndex).asPrimitiveType()), version);

                for(int i = 0 ; i < reader.getTotalValueCount();i++) {
                    if(reader.getCurrentDefinitionLevel()< descriptor.getMaxDefinitionLevel()) {
//                        reader.skip();
                    } else {
                        reader.writeCurrentValueToConverter();
                    }
                    reader.consume();
                }
            }
        });
    }
}
