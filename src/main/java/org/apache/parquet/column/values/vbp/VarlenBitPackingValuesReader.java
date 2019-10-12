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

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetEncodingException;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * See <code>VarlenBitPackingValuesWriter</code>
 */
public class VarlenBitPackingValuesReader extends ValuesReader {

    private int nextOffset;
    private int numGroup;
    private int valueRemain;

    private int currentGroupRemain;
    private int currentBitWidth;
    private int currentGroupBase;
    private BytePacker packer;

    private byte[] packedBuffer = new byte[128];
    private int[] unpackBuffer = new int[32];
    // The offset of unpack buffer in the 512 number group
    private int bufferPointer;
    private int bufferSize;

    private ByteBufferInputStream input;

    public VarlenBitPackingValuesReader() {

    }

    @Override
    public void initFromPage(int valueCount, ByteBuffer page, int offset) throws IOException {
        this.valueRemain = valueCount;
        input = new ByteBufferInputStream(page, offset, page.limit() - offset);

        int length = BytesUtils.readIntLittleEndian(input);

        nextOffset = offset + 8 + length;
        readNextGroup();
    }


    void readNextGroup() {
        try {
            currentBitWidth = input.read();
            currentGroupRemain = Math.min(512, valueRemain);
            currentGroupBase = BytesUtils.readZigZagVarInt(input);
            packer = Packer.BIG_ENDIAN.newBytePacker(currentBitWidth);

            valueRemain -= currentGroupRemain;
            readNext32Number();
        } catch (IOException e) {
            throw new ParquetEncodingException("can not read block", e);
        }
    }

    void readNext32Number() {
        try {
            input.read(packedBuffer, 0, currentBitWidth * 4);
            packer.unpack32Values(packedBuffer, 0, unpackBuffer, 0);
            bufferSize = Math.min(currentGroupRemain, 32);
            currentGroupRemain -= bufferSize;

            bufferPointer = 0;
        } catch (IOException e) {
            throw new ParquetEncodingException("can not read stripe", e);
        }
    }

    @Override
    public void skip() {
        readInteger();
    }

    @Override
    public void skip(long numRecords) {
        try {
            int remain = (int) numRecords;
            // Still in current buffer
            if (bufferPointer + numRecords < bufferSize) {
                bufferPointer += numRecords;
            } else if (numRecords < currentGroupRemain) {
                // Still within current group
                remain -= (bufferSize - bufferPointer - 1);
                input.skip(((remain >> 5) << 5) * currentBitWidth);

                readNext32Number();
                bufferPointer = remain & 0x1F;
            } else { // Calculate the target group and stripe
                remain -= currentGroupRemain;
                input.skip(((currentGroupRemain >> 5) << 5) * currentBitWidth);
                while (remain >= 0x1FF) {
                    // skip the whole group
                    currentBitWidth = input.read();
                    currentGroupBase = BytesUtils.readZigZagVarInt(input);
                    input.skip(currentBitWidth << 6);
                    remain -= 0x1FF;
                }
            }
        } catch (IOException e) {
            throw new ParquetEncodingException("can not skip records", e);
        }
    }

    @Override
    public int readInteger() {
        if (bufferPointer == bufferSize) {
            if (currentGroupRemain == 0) {
                readNextGroup();
            } else {
                readNext32Number();
            }
        }
        return currentGroupBase + unpackBuffer[bufferPointer++];
    }

    @Override
    public int getNextOffset() {
        return nextOffset;
    }
}
