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

import edu.uchicago.cs.encsel.model.LongEncoding;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

public class OrcWriterHelper {

    public static File genOutput(URI input, String suffix) {
        try {
            if (input.getPath().endsWith("\\.data")) {
                return new File(new URI(input.toString().replaceFirst("data$", suffix)));
            }
            return new File(new URI(input.toString() + "." + suffix));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    static final String NULL = "null";

    public static URI singleColumnBoolean(URI input) throws IOException {
        File output = genOutput(input, "ORC");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        Writer writer = OrcWriterBuilder.buildWriter(new Path(output.toURI()), "struct<x:boolean>");

        TypeDescription schema = writer.getSchema();

        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector first = (LongColumnVector) batch.cols[0];

        final int BATCH_SIZE = batch.getMaxSize();

        String line;
        while ((line = reader.readLine()) != null) {
            int row = batch.size++;

            line = line.trim();
            if (line.isEmpty() || line.equals(NULL)) {
                first.noNulls = false;
                first.isNull[row] = true;
            } else {
                first.vector[row] = Integer.parseInt(line);
            }

            if (row == BATCH_SIZE - 1) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnInt(URI input) throws IOException {
        File output = genOutput(input, "ORC");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        Writer writer = OrcWriterBuilder.buildWriter(new Path(output.toURI()), "struct<x:int>");

        TypeDescription schema = writer.getSchema();

        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector first = (LongColumnVector) batch.cols[0];

        final int BATCH_SIZE = batch.getMaxSize();

        String line;
        while ((line = reader.readLine()) != null) {
            int row = batch.size++;
            line = line.trim();
            if (line.isEmpty() || line.equals(NULL)) {
                first.noNulls = false;
                first.isNull[row] = true;
            } else {
                first.vector[row] = Integer.parseInt(line);
            }
            if (row == BATCH_SIZE - 1) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnLong(URI input) throws IOException {
        File output = genOutput(input, "ORC");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        Writer writer = OrcWriterBuilder.buildWriter(new Path(output.toURI()), "struct<x:bigint>");

        TypeDescription schema = writer.getSchema();

        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector first = (LongColumnVector) batch.cols[0];

        final int BATCH_SIZE = batch.getMaxSize();

        String line;
        while ((line = reader.readLine()) != null) {
            int row = batch.size++;
            line = line.trim();
            if (line.isEmpty() || line.equals(NULL)) {
                first.noNulls = false;
                first.isNull[row] = true;
            } else {
                first.vector[row] = Integer.parseInt(line);
            }
            if (row == BATCH_SIZE - 1) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnString(URI input) throws IOException {
        File output = genOutput(input, "ORC");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        Writer writer = OrcWriterBuilder.buildWriter(new Path(output.toURI()), "struct<x:string>");

        TypeDescription schema = writer.getSchema();

        VectorizedRowBatch batch = schema.createRowBatch();
        BytesColumnVector first = (BytesColumnVector) batch.cols[0];

        final int BATCH_SIZE = batch.getMaxSize();

        String line;
        while ((line = reader.readLine()) != null) {
            int row = batch.size++;

            if (line.isEmpty() || line.equals(NULL)) {
                first.noNulls = false;
                first.isNull[row] = true;
            } else {
                first.setVal(row, line.getBytes(StandardCharsets.UTF_8));
            }
            if (row == BATCH_SIZE - 1) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnDouble(URI input) throws IOException {
        File output = genOutput(input, "ORC");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        Writer writer = OrcWriterBuilder.buildWriter(new Path(output.toURI()), "struct<x:double>");

        TypeDescription schema = writer.getSchema();

        VectorizedRowBatch batch = schema.createRowBatch();
        DoubleColumnVector first = (DoubleColumnVector) batch.cols[0];

        final int BATCH_SIZE = batch.getMaxSize();

        String line;
        while ((line = reader.readLine()) != null) {
            int row = batch.size++;
            line = line.trim();
            if (line.isEmpty() || line.equals(NULL)) {
                first.noNulls = false;
                first.isNull[row] = true;
            } else {
                first.vector[row] = Double.parseDouble(line);
            }
            if (row == BATCH_SIZE - 1) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnFloat(URI input) throws IOException {
        File output = genOutput(input, "ORC");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        Writer writer = OrcWriterBuilder.buildWriter(new Path(output.toURI()), "struct<x:float>");

        TypeDescription schema = writer.getSchema();

        VectorizedRowBatch batch = schema.createRowBatch();
        DoubleColumnVector first = (DoubleColumnVector) batch.cols[0];

        final int BATCH_SIZE = batch.getMaxSize();

        String line;
        while ((line = reader.readLine()) != null) {
            int row = batch.size++;
            line = line.trim();
            if (line.isEmpty() || line.equals(NULL)) {
                first.noNulls = false;
                first.isNull[row] = true;
            } else {
                first.vector[row] = Double.parseDouble(line);
            }
            if (row == BATCH_SIZE - 1) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        reader.close();
        writer.close();

        return output.toURI();
    }
}
