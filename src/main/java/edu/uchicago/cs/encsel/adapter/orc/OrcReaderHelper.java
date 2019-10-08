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

package edu.uchicago.cs.encsel.adapter.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class OrcReaderHelper {

    public static void readIntColumn(URI inputFile, int columnIndex, ReaderCallback callback) throws IOException {
        Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(new Path(inputFile), OrcFile.readerOptions(conf));

        callback.init();

        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        while (rows.nextBatch(batch)) {
            for (int r = 0; r < batch.size; ++r) {
                LongColumnVector content = (LongColumnVector) batch.cols[columnIndex];
                callback.onNextInt((int) content.vector[r]);
            }
        }
        rows.close();
        callback.done();
    }

    public static void readStringColumn(URI inputFile, int columnIndex, ReaderCallback callback) throws IOException {
        Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(new Path(inputFile), OrcFile.readerOptions(conf));

        callback.init();

        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        while (rows.nextBatch(batch)) {
            for (int r = 0; r < batch.size; ++r) {
                BytesColumnVector content = (BytesColumnVector) batch.cols[columnIndex];
                callback.onNextString(new String(content.vector[0],
                        content.start[r], content.length[r], StandardCharsets.UTF_8));
            }
        }
        rows.close();
        callback.done();
    }

    public static void readDoubleColumn(URI inputFile, int columnIndex, ReaderCallback callback) throws IOException {
        Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(new Path(inputFile), OrcFile.readerOptions(conf));

        callback.init();

        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        while (rows.nextBatch(batch)) {
            for (int r = 0; r < batch.size; ++r) {
                DoubleColumnVector content = (DoubleColumnVector) batch.cols[columnIndex];
                callback.onNextDouble(content.vector[r]);
            }
        }
        rows.close();
        callback.done();
    }
}
