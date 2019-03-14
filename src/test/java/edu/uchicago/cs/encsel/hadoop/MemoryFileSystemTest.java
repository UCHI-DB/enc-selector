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

package edu.uchicago.cs.encsel.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import sun.nio.ch.IOUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.net.URI;

import static org.junit.Assert.assertEquals;

public class MemoryFileSystemTest {

    @Test
    public void testListFileStatus() throws Exception {
        URI localpath = new File("src/test/resource/query_select/customer_100").toURI();
        URI memorypath = new URI("memory://" + localpath.getPath());
        Configuration conf = new Configuration();
        conf.set("fs.memory.impl", MemoryFileSystem.class.getName());
        FileStatus[] fs = FileSystem.get(memorypath, conf)
                .listStatus(new Path("memory", "", localpath.getPath()));
        assertEquals(1, fs.length);
        assertEquals("memory:" + localpath.getPath(), fs[0].getPath().toString());
    }

    @Test
    public void testOpen() throws Exception {
        URI localpath = new File("src/test/resource/query_select/customer_100").toURI();
        URI memorypath = new URI("memory://" + localpath.getPath());

        Configuration conf = new Configuration();
        conf.set("fs.memory.impl", MemoryFileSystem.class.getName());

        FSDataInputStream input = FileSystem.get(memorypath, conf).open(new Path(memorypath));

//        assertTrue(input.getFileDescriptor().valid());
        assertEquals(0, input.getPos());

        ByteArrayOutputStream memresult = new ByteArrayOutputStream();
        IOUtils.copy(input, memresult);

        ByteArrayOutputStream localresult = new ByteArrayOutputStream();
        IOUtils.copy(new FileInputStream(new File(localpath)), localresult);

        byte[] localarray = localresult.toByteArray();
        byte[] memarray = memresult.toByteArray();
        assertEquals(localarray.length, memarray.length);
        for (int i = 0; i < localarray.length; i++) {
            assertEquals(localarray[i], memarray[i]);
        }
    }
}
