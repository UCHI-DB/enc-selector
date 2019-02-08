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

import edu.uchicago.cs.encsel.model.FloatEncoding;
import edu.uchicago.cs.encsel.model.IntEncoding;
import edu.uchicago.cs.encsel.model.StringEncoding;
import edu.uchicago.cs.encsel.parquet.EncReaderProcessor;
import edu.uchicago.cs.encsel.parquet.ParquetReaderHelper;
import edu.uchicago.cs.encsel.util.perf.ProfileBean;
import edu.uchicago.cs.encsel.util.perf.Profiler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.File;
import java.net.URI;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TPCHLoadParquetInMem {

    static final CompressionCodecName[] codecs =
            {CompressionCodecName.UNCOMPRESSED, CompressionCodecName.LZO, CompressionCodecName.GZIP};

    public static void main(String[] args) throws Exception {
        File file = new File(args[0]);

        MessageType lineitemSchema = TPCHSchema.lineitemSchema();

        for(int i = 0 ; i < lineitemSchema.getColumns().size();i++) {
            ColumnDescriptor cd = lineitemSchema.getColumns().get(i);
            readColumn(file, i+1, cd);
        }
    }

    protected static void readColumn(File main, int index, ColumnDescriptor cd) throws Exception {
        List<String> encodings = null;
        switch(cd.getType()) {
            case BINARY:
                encodings = Arrays.stream(StringEncoding.values())
                        .filter(p->p.parquetEncoding()!=null).map(e->e.name()).collect(Collectors.toList());
                break;
            case INT32:
                encodings = Arrays.stream(IntEncoding.values())
                        .filter(p->p.parquetEncoding()!=null).map(e->e.name()).collect(Collectors.toList());
                break;
            case DOUBLE:
                encodings = Arrays.stream(FloatEncoding.values())
                        .filter(p->p.parquetEncoding()!=null).map(e->e.name()).collect(Collectors.toList());
                break;
            default:
               encodings = Collections.<String>emptyList();
               break;
        }

        for(String e: encodings) {
            for(CompressionCodecName codec : codecs) {
                String fileName = MessageFormat.format("{0}.col{1}.{2}_{3}",main.getAbsolutePath(), index,e,codec.name());
                ProfileBean loadTime = readEncoding(new File(fileName).toURI());
                System.out.println(MessageFormat.format("{0}, {1}, {2}, {3}", index, e, codec.name(),
                        String.valueOf(loadTime.wallclock())));
            }
        }
    }

    protected static ProfileBean readEncoding(URI file) throws Exception {
        Profiler p = new Profiler();
        p.mark();

        Configuration conf = new Configuration();
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);
        List<FileStatus> statuses = Arrays.asList(fs.listStatus(path, HiddenFileFilter.INSTANCE));
        List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);
        if (footers.isEmpty()) {
            return p.stop();
        }

        EncReaderProcessor processor = new EncReaderProcessor() {
            @Override
            public void processRowGroup(VersionParser.ParsedVersion version, BlockMetaData meta, PageReadStore rowGroup) {

            }
        };

        for (Footer footer : footers) {
            processor.processFooter(footer);

            VersionParser.ParsedVersion version = VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());

            ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile(), footer.getParquetMetadata());
            PageReadStore rowGroup = null;
            int blockCounter = 0;
            List<ColumnDescriptor> cols = footer.getParquetMetadata().getFileMetaData().getSchema().getColumns();
            while ((rowGroup = fileReader.readNextRowGroup()) != null) {
                blockCounter++;
            }
        }

        return p.stop();
    }
}
