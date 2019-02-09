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

import edu.uchicago.cs.encsel.model.FloatEncoding;
import edu.uchicago.cs.encsel.model.IntEncoding;
import edu.uchicago.cs.encsel.model.StringEncoding;
import edu.uchicago.cs.encsel.parquet.EncReaderProcessor;
import edu.uchicago.cs.encsel.parquet.ParquetReaderHelper;
import edu.uchicago.cs.encsel.query.ColumnPrimitiveConverter;
import edu.uchicago.cs.encsel.query.NonePrimitiveConverter;
import edu.uchicago.cs.encsel.util.perf.ProfileBean;
import edu.uchicago.cs.encsel.util.perf.Profiler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.net.URI;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TPCHColumnMaterialize {

    public static void main(String[] args) throws Exception {
        File file = new File(args[0]);
        final int numThread = Integer.valueOf(args[1]);
        new TPCHWorker(new EncReaderProcessor() {
            @Override
            public int expectNumThread() {
                return numThread;
            }

            @Override
            public void processRowGroup(VersionParser.ParsedVersion version,
                                        BlockMetaData meta, PageReadStore rowGroup) {
                for (ColumnDescriptor cd : schema.getColumns()) {
                    ColumnReader cr = new ColumnReaderImpl(cd, rowGroup.getPageReader(cd),
                            new NonePrimitiveConverter(), version);

                    if (cr.getCurrentDefinitionLevel() == cr.getDescriptor().getMaxDefinitionLevel()) {
                        cr.writeCurrentValueToConverter();
                    }
                }
            }
        }, TPCHSchema.lineitemSchema());
    }

}
