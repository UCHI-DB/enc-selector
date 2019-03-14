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

import sun.misc.Cleaner;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;

class DirectByteArray {

    private final long startIndex;

    private long size;

    private final Cleaner cleaner;

    static Unsafe unsafe = getUnsafe();

    private static Unsafe getUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public DirectByteArray(long size) {
        this.size = size;
        startIndex = unsafe.allocateMemory(size);
//        unsafe.setMemory(startIndex, size, (byte) 0);

        cleaner = Cleaner.create(this, new Deallocator());
    }

    public void set(long index, byte value) {
        if (index >= size)
            throw new ArrayIndexOutOfBoundsException();
        unsafe.putByte(startIndex + index, value);
    }

    public int get(long index) {
        if (index >= size)
            throw new ArrayIndexOutOfBoundsException();
        return unsafe.getByte(startIndex + index);
    }

    public void destroy() {
        unsafe.freeMemory(startIndex);
    }

    protected int copylen(long index, int length) {
        int copylen = length;
        if (index >= size) {
            return -1;
        }
        if (index + length > size)
            copylen = (int) (size - index);
        return copylen;
    }

    public int read(long index, byte[] buffer, int offset, int length) {
        int copylen = copylen(index, length);
        if (copylen <= 0)
            return copylen;
        unsafe.copyMemory(null, startIndex + index,
                buffer, unsafe.arrayBaseOffset(byte[].class) + offset, copylen);
        return copylen;
    }

    public int write(long index, byte[] buffer, int offset, int length) {
        int copylen = copylen(index, length);
        if (copylen <= 0)
            return copylen;
        unsafe.copyMemory(buffer, unsafe.arrayBaseOffset(byte[].class) + offset,
                null, startIndex + index, copylen);
        return copylen;
    }

    public long size() {
        return size;
    }

    public void from(InputStream from, long size) throws IOException {
        if (size > this.size)
            throw new ArrayIndexOutOfBoundsException();
        byte[] buffer = new byte[1000000];
        long position = 0;
        int readcount = 0;
        while ((readcount = from.read(buffer)) > 0) {
            write(position, buffer, 0, readcount);
            position += readcount;
        }
    }

    private class Deallocator implements Runnable {

        private Deallocator() {
        }

        public void run() {
            DirectByteArray.this.destroy();
        }

    }
}
