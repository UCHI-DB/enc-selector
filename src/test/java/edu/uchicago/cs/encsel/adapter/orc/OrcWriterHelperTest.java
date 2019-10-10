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

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static edu.uchicago.cs.encsel.adapter.orc.OrcWriterHelper.NULL;
import static org.junit.Assert.*;

public class OrcWriterHelperTest {

    @Test
    public void singleColumnInt() throws Exception {
        URI file = new File("src/test/resource/orc/int_column").toURI();
        Files.deleteIfExists(Paths.get(file));

        // Prepare test file
        PrintWriter pw = new PrintWriter(new FileOutputStream(new File(file)));
        int seed = 4942;

        for (int i = 0; i < 10000; i++) {
            if (i % 10 == 0) {
                pw.println("null");
            } else {
                pw.println(String.valueOf(i + seed));
            }
        }
        pw.close();

        OrcWriterHelper.singleColumnInt(file);

        URI expectFile = new File("src/test/resource/orc/int_column.ORC").toURI();
        OrcReaderHelper.readIntColumn(expectFile, 0, new ReaderCallback() {
            int counter = 0;

            @Override
            public void onNextInt(int value) {
                assertEquals(value, seed + counter++);
            }

            @Override
            public void onNextNull() {
                assertTrue(counter % 10 == 0);
                counter++;
            }
        });
    }

    @Test
    public void singleColumnString() throws Exception {
        URI file = new File("src/test/resource/orc/str_column").toURI();
        Files.deleteIfExists(Paths.get(file));

        // Prepare test file
        PrintWriter pw = new PrintWriter(new FileOutputStream(new File(file)));

        List<String> content = new ArrayList<>();

        for (int i = 0; i < 10000; i++) {
            if (i % 7 == 0) {
                pw.println(NULL);
                content.add(null);
            } else {
                String uuid = UUID.randomUUID().toString();
                pw.println(uuid);
                content.add(uuid);
            }
        }
        pw.close();

        OrcWriterHelper.singleColumnString(file);

        URI expectFile = new File("src/test/resource/orc/str_column.ORC").toURI();
        OrcReaderHelper.readStringColumn(expectFile, 0, new ReaderCallback() {
            int counter = 0;

            @Override
            public void onNextString(String value) {
                assertEquals(content.get(counter++), value);
            }

            @Override
            public void onNextNull() {
                assertNull(content.get(counter++));
            }
        });
    }

    @Test
    public void singleColumnDouble() throws Exception {
        URI file = new File("src/test/resource/orc/double_column").toURI();
        Files.deleteIfExists(Paths.get(file));

        // Prepare test file
        PrintWriter pw = new PrintWriter(new FileOutputStream(new File(file)));

        List<Double> content = new ArrayList<>();
        Random rand = new Random(System.currentTimeMillis());

        for (int i = 0; i < 10000; i++) {
            if (i % 19 == 0) {
                pw.println(NULL);
                content.add(null);
            } else {
                double value = rand.nextDouble();
                pw.println(String.valueOf(value));
                content.add(value);
            }
        }
        pw.close();

        OrcWriterHelper.singleColumnDouble(file);

        URI expectFile = new File("src/test/resource/orc/double_column.ORC").toURI();
        OrcReaderHelper.readDoubleColumn(expectFile, 0, new ReaderCallback() {
            int counter = 0;

            @Override
            public void onNextDouble(double value) {
                assertEquals(content.get(counter++), value, 0.0001);
            }

            @Override
            public void onNextNull() {
                assertTrue(counter % 19 == 0);
                assertNull(content.get(counter++));
            }
        });
    }
}