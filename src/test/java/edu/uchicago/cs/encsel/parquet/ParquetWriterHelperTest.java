/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 *******************************************************************************/
package edu.uchicago.cs.encsel.parquet;

import edu.uchicago.cs.encsel.model.IntEncoding;
import edu.uchicago.cs.encsel.model.StringEncoding;
import edu.uchicago.cs.encsel.query.tpch.TPCHSchema;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.it.unimi.dsi.fastutil.doubles.Double2IntMap;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.Int2IntMap;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParquetWriterHelperTest {

    @Before
    public void deleteFile() throws IOException {
        Files.deleteIfExists(Paths.get("src/test/resource/coldata/test_col_str.data.DELTAL"));
        Files.deleteIfExists(Paths.get("src/test/resource/coldata/test_col_str.data.DICT"));
        Files.deleteIfExists(Paths.get("src/test/resource/coldata/test_col_str.data.PLAIN"));

        Files.deleteIfExists(Paths.get("src/test/resource/coldata/test_col_int.data.DICT"));
        Files.deleteIfExists(Paths.get("src/test/resource/coldata/test_col_int.data.BP"));
        Files.deleteIfExists(Paths.get("src/test/resource/coldata/test_col_int.data.DELTABP"));
        Files.deleteIfExists(Paths.get("src/test/resource/coldata/test_col_int.data.RLE"));
        Files.deleteIfExists(Paths.get("src/test/resource/coldata/test_col_int.data.PLAIN"));
    }

    @Test
    public void testWriteStr() throws IOException {
        String file = "src/test/resource/coldata/test_col_str.data";

        ParquetWriterHelper.singleColumnString(new File(file).toURI(), StringEncoding.DICT);
        ParquetWriterHelper.singleColumnString(new File(file).toURI(), StringEncoding.DELTAL);
        ParquetWriterHelper.singleColumnString(new File(file).toURI(), StringEncoding.PLAIN);

        assertTrue(Files.exists(Paths.get("src/test/resource/coldata/test_col_str.data.DELTAL")));
        assertTrue(Files.exists(Paths.get("src/test/resource/coldata/test_col_str.data.DICT")));
        assertTrue(Files.exists(Paths.get("src/test/resource/coldata/test_col_str.data.PLAIN")));
    }

    @Test
    public void testWriteInt() throws IOException, VersionParser.VersionParseException {
        String file = "src/test/resource/coldata/test_col_int.data";

        ParquetWriterHelper.singleColumnInt(new File(file).toURI(), IntEncoding.DICT);
        ParquetWriterHelper.singleColumnInt(new File(file).toURI(), IntEncoding.BP);
        ParquetWriterHelper.singleColumnInt(new File(file).toURI(), IntEncoding.DELTABP);
        ParquetWriterHelper.singleColumnInt(new File(file).toURI(), IntEncoding.RLE);
        ParquetWriterHelper.singleColumnInt(new File(file).toURI(), IntEncoding.PLAIN);


        assertTrue(Files.exists(Paths.get("src/test/resource/coldata/test_col_int.data.DICT")));
        assertTrue(Files.exists(Paths.get("src/test/resource/coldata/test_col_int.data.BP")));
        assertTrue(Files.exists(Paths.get("src/test/resource/coldata/test_col_int.data.DELTABP")));
        assertTrue(Files.exists(Paths.get("src/test/resource/coldata/test_col_int.data.RLE")));
        assertTrue(Files.exists(Paths.get("src/test/resource/coldata/test_col_int.data.PLAIN")));


        // Check whether context information has been stored to footer
        ParquetReaderHelper.read(new File(
                        "src/test/resource/coldata/test_col_int.data.BP").toURI(),
                new ReaderProcessor() {
                    @Override
                    public void processFooter(Footer footer) {
                        Map<String, String> meta = footer.getParquetMetadata().getFileMetaData().getKeyValueMetaData();
                        assertEquals("26", meta.get("[value] INT32.0"));
                        assertEquals("67108863", meta.get("[value] INT32.1"));
                    }

                    @Override
                    public void processRowGroup(VersionParser.ParsedVersion version,
                                                BlockMetaData meta, PageReadStore rowGroup) {
                    }
                });

        ParquetReaderHelper.read(new File(
                        "src/test/resource/coldata/test_col_int.data.RLE").toURI(),
                new ReaderProcessor() {
                    @Override
                    public void processFooter(Footer footer) {
                        Map<String, String> meta = footer.getParquetMetadata().getFileMetaData().getKeyValueMetaData();
                        assertEquals("26", meta.get("[value] INT32.0"));
                        assertEquals("67108863", meta.get("[value] INT32.1"));
                    }

                    @Override
                    public void processRowGroup(VersionParser.ParsedVersion version,
                                                BlockMetaData meta, PageReadStore rowGroup) {
                    }
                });
    }

    @Test
    public void testDetermineBitLength() {
        assertEquals(13, ParquetWriterHelper.scanIntBitLength(new File("src/test/resource/coldata/bitlength_test").toURI()));
    }

    @Test
    public void testEncodeBoolean() throws IOException {
        String file = "src/test/resource/coldata/test_col_boolean.data";

        ParquetWriterHelper.singleColumnBoolean(new File(file).toURI());
        assertTrue(Files.exists(Paths.get("src/test/resource/coldata/test_col_boolean.data.PLAIN")));

    }
    @Test
    public void testBuildGlobalDict(){
        String lineitem = "../tpch-generator/dbgen/lineitem";
        String part = "../tpch-generator/dbgen/part";
        Object2IntMap dict = ParquetWriterHelper.buildGlobalDict(new File(part+".tbl").toURI(), 2, TPCHSchema.partSchema());
        System.out.println(dict.containsKey(Binary.fromString("SMAL BRUSHED COPPER")));
        System.out.println(dict);
        ObjectIterator<Object2IntMap.Entry> entryIterator = dict.object2IntEntrySet().iterator();
        while (entryIterator.hasNext()) {
            Object2IntMap.Entry entry = entryIterator.next();
            System.out.println(entry.getKey());
        }

    }
}