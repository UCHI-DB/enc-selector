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

package org.apache.parquet.column.values.vbp;

import org.apache.parquet.Ints;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetEncodingException;

import java.io.IOException;

/**
 * Variable bitWidth bit-packing encoding
 * <p>
 * The input is split into groups, each group contains at most 512 numbers.
 * The groups are offset to the minimal number and then bit-packed
 * using their minimal available bit-width.
 * <p>
 * The structure is described as follows
 * <p>
 * data = length group+
 * group = {bit-width}{base-number}{bit-packed data}
 * <p>
 * The bit-width is a single byte and base number is full-width integers.
 * and the actual number of data follows the base number.
 */
public class VarlenBitPackingValuesWriter extends ValuesWriter {

    int slabSize;
    int pageSize;
    ByteBufferAllocator allocator;
    CapacityByteArrayOutputStream output;

    int[] buffer = new int[512];
    int bufferPointer = 0;
    byte[] packedBuffer = new byte[2048];

    public VarlenBitPackingValuesWriter(int slabSize, int pageSize, ByteBufferAllocator allocator) {
        this.slabSize = slabSize;
        this.pageSize = pageSize;
        this.allocator = allocator;
        this.output = new CapacityByteArrayOutputStream(slabSize, pageSize, allocator);
    }

    @Override
    public long getBufferedSize() {
        return output.size();
    }

    @Override
    public void writeInteger(int v) {
        buffer[bufferPointer++] = v;
        if (bufferPointer == buffer.length) {
            writeBlock();
        }
    }

    @Override
    public BytesInput getBytes() {
        if (bufferPointer > 0) {
            writeBlock();
        }
        BytesInput content = BytesInput.from(output);
        return BytesInput.concat(BytesInput.fromInt(Ints.checkedCast(content.size())),
                content);
    }

    private void writeBlock() {
        // Find the minimal number
        int min = Integer.MAX_VALUE;
        for (int i = 0 ; i < bufferPointer;i++) {
            min = Math.min(min, buffer[i]);
        }
        // Subtract min, and obtain the max
        int maxremain = Integer.MIN_VALUE;
        for (int i = 0; i < bufferPointer; i++) {
            buffer[i] -= min;
            if(buffer[i] < 0)
                throw new IllegalArgumentException("number overflow found, " +
                        "consider using long version of this writer, which is not yet available.");
            maxremain = Math.max(maxremain, buffer[i]);
        }
        // Bit width
        int bitWidth = Integer.bitCount((Integer.highestOneBit(maxremain) << 1) - 1);

        try {
            // Write header to the output
            output.write(bitWidth);
            BytesUtils.writeZigZagVarInt(min, output);

            // Write bit-packed data block
            BytePacker packer = Packer.BIG_ENDIAN.newBytePacker(bitWidth);

            // Pack 16 times for 512 values
            int rounds = (bufferPointer + 31) >> 5;
            for (int i = 0; i < rounds; i++) {
                packer.pack32Values(buffer, i * 32, packedBuffer, i * 4 * bitWidth);
            }
            // Always write block of 512 numbers, which has size of bitWidth*64 bytes
            output.write(packedBuffer, 0, bitWidth * 64);
        } catch (IOException e) {
            throw new ParquetEncodingException("can not write block", e);
        }
        bufferPointer = 0;
    }

    @Override
    public Encoding getEncoding() {
        return Encoding.VARLEN_BIT_PACKED;
    }

    @Override
    public void reset() {
        output.reset();
        bufferPointer = 0;
    }

    @Override
    public long getAllocatedSize() {
        return output.getCapacity();
    }

    @Override
    public String memUsageString(String prefix) {
        return String.format("%s VarlenBinaryPacking %d bytes", prefix, getAllocatedSize());
    }
}
