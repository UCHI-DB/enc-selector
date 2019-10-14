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
package org.apache.parquet.column.values.rle;

import org.apache.parquet.Log;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.apache.parquet.Log.DEBUG;

/**
 * Decodes values written in the grammar described in {@link RunLengthBitPackingHybridEncoder}
 *
 * @author Julien Le Dem
 * @author Modified by Harper to support multiple records skipping
 */
public class RunLengthBitPackingHybridDecoder {
    private static final Log LOG = Log.getLog(org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder.class);

    private static enum MODE {RLE, PACKED}

    private final int bitWidth;
    private final BytePacker packer;
    private final InputStream in;

    private MODE mode;
    private int currentCount;
    private int currentEntrySize;

    // state for RLE
    private int currentValue;

    // state for PACKED
    private int[] currentBuffer = new int[8];
    private int currentGroup = 0;

    private byte[] byteBuffer;

    public RunLengthBitPackingHybridDecoder(int bitWidth, InputStream in) {
        if (DEBUG) LOG.debug("decoding bitWidth " + bitWidth);

        Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
        this.bitWidth = bitWidth;
        this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
        this.in = in;
        this.byteBuffer = new byte[64 * bitWidth];
    }

    public int readInt() throws IOException {
        if (currentCount == 0) {
            readNext();
        }
        --currentCount;
        int result;
        switch (mode) {
            case RLE:
                result = currentValue;
                break;
            case PACKED:
                int offset = currentEntrySize - currentCount - 1;
                int groupIndex = offset >>> 3;
                if (groupIndex != currentGroup) {
                    packer.unpack8Values(byteBuffer, groupIndex * bitWidth, currentBuffer, 0);
                    currentGroup = groupIndex;
                }
                result = currentBuffer[offset & 0x7];
                break;
            default:
                throw new ParquetDecodingException("not a valid mode " + mode);
        }
        return result;
    }

    private void readNext() throws IOException {
        Preconditions.checkArgument(in.available() > 0, "Reading past RLE/BitPacking stream.");
        final int header = BytesUtils.readUnsignedVarInt(in);
        mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
        switch (mode) {
            case RLE:
                currentCount = header >>> 1;
                if (DEBUG) LOG.debug("reading " + currentCount + " values RLE");
                currentValue = BytesUtils.readIntLittleEndianPaddedOnBitWidth(in, bitWidth);
                break;
            case PACKED:
                int numGroups = header >>> 1;
                currentCount = numGroups << 3;
                readBuffer();
                currentGroup = -1;
                break;
            default:
                throw new ParquetDecodingException("not a valid mode " + mode);
        }
        currentEntrySize = currentCount;
    }

    /**
     * @param numRecords
     * @throws IOException
     */
    public void skip(long numRecords) throws IOException {
        if (0 == numRecords) {
            return;
        }
        long remain = numRecords;
        if (currentCount > 0) {
            long moveForward = Math.min(currentCount, remain);
            remain -= moveForward;
            currentCount -= moveForward;
        }
        if (remain == 0) {
            return;
        }
        while (true) {
            // Load next entry
            int entryCount = readNextHeader();
            if (remain < entryCount) {
                // Cannot use readNext as we have consumed the header
                readNextBody(entryCount);
                currentCount -= remain;
                break;
            }
            remain -= entryCount;
            switch (mode) {
                case RLE:
                    in.skip(BytesUtils.paddedByteCountFromBits(bitWidth));
                    break;
                case PACKED:
                    in.skip((entryCount >>> 3) * bitWidth);
                    break;
            }
        }
    }

    void unpack() throws IOException {
        readBuffer();
        for (int valueIndex = 0, byteIndex = 0; valueIndex < currentCount; valueIndex += 8, byteIndex += bitWidth) {
            packer.unpack8Values(byteBuffer, byteIndex, currentBuffer, valueIndex);
        }
    }

    void readBuffer() throws IOException {
        int numGroups = currentCount >> 3;
        if (DEBUG) LOG.debug("reading " + currentCount + " values BIT PACKED");
        // At the end of the file RLE data though, there might not be that many bytes left.
        int bytesToRead = numGroups * bitWidth;
        in.read(byteBuffer, 0, bytesToRead);
    }

    int readNextHeader() throws IOException {
        // Load next entry
        Preconditions.checkArgument(in.available() > 0, "Skipping past RLE/BitPacking stream.");
        final int header = BytesUtils.readUnsignedVarInt(in);
        mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
        switch (mode) {
            case RLE:
                return header >>> 1;
            case PACKED:
                int numGroups = header >>> 1;
                return numGroups << 3;
            default:
                return -1;
        }
    }

    void readNextBody(int entryCount) throws IOException {
        currentCount = entryCount;
        currentEntrySize = currentCount;
        switch (mode) {
            case RLE:
                currentValue = BytesUtils.readIntLittleEndianPaddedOnBitWidth(in, bitWidth);
                break;
            case PACKED:
                readBuffer();
                currentGroup = -1;
                break;
            default:
                break;
        }
    }

    /**
     * This method is designed to speed up the skipping of 0-1 definition level encoded in RLE.
     * Result is undefined for bitWidth > 1
     *
     * @param numRecords
     * @return num of non-zero
     * @throws IOException
     */
    public long skipWithCount(long numRecords) throws IOException {
        if (0 == numRecords) {
            return 0;
        }
        long remain = numRecords;
        long nonzeroCounter = 0;
        if (currentCount > 0) {
            long moveForward = Math.min(currentCount, remain);
            remain -= moveForward;
            nonzeroCounter += skipCountInEntry(moveForward);
        }
        if (remain == 0)
            return nonzeroCounter;
        while (true) {
            // Load next entry
            int entryCount = readNextHeader();
            if (remain < entryCount) {
                // Cannot use readNext as we have consumed the header
                readNextBody(entryCount);
                nonzeroCounter += skipCountInEntry(remain);
                break;
            }
            remain -= entryCount;

            switch (mode) {
                case RLE:
                    currentValue = BytesUtils.readIntLittleEndianPaddedOnBitWidth(in, bitWidth);
                    nonzeroCounter += (currentValue == 0 ? 0 : 1) * entryCount;
                    break;
                case PACKED:
                    // Read in data and use bitCount to count non-zero values
                    int numBytes = (entryCount >>> 3) * bitWidth;
                    in.read(byteBuffer, 0, numBytes);
                    nonzeroCounter += nonzeroBits(numBytes);
                    break;
            }
        }
        return nonzeroCounter;
    }

    protected long skipCountInEntry(long moveForward) {
        long skipped = 0;
        switch (mode) {
            case RLE:
                skipped = moveForward * (currentValue == 0 ? 0 : 1);
                break;
            case PACKED:
                skipped = nonzeroBits(currentEntrySize - currentCount, (int) moveForward);
                break;
            default:
                break;
        }
        currentCount -= moveForward;
        return skipped;
    }

    /**
     * Count number of non-zero bits in the given range of buffer
     *
     * @param offset
     * @param numBits
     * @return
     */
    protected int nonzeroBits(int offset, int numBits) {
        int bitCount = 0;

        ByteBuffer wrapper = ByteBuffer.wrap(byteBuffer);
        wrapper.order(ByteOrder.LITTLE_ENDIAN);

        int startLong = offset >>> 6;
        int startOffset = offset & 0x3F;
        int stopLong = (offset + numBits) >>> 6;
        int stopOffset = (offset + numBits) & 0x3F;

        long headMask = -1L << startOffset;
        long tailMask = (1L << stopOffset) - 1;
        if (startLong == stopLong) {// A single long, Merge head and tail mask
            bitCount += Long.bitCount(wrapper.getLong(startLong << 3) & tailMask & headMask);
        } else {
            bitCount += Long.bitCount(wrapper.getLong(startLong << 3) & headMask);
            bitCount += Long.bitCount(wrapper.getLong(stopLong << 3) & tailMask);
            for (int i = startLong + 1; i < stopLong; i++) {
                bitCount += Long.bitCount(wrapper.getLong(i << 3));
            }
        }
        return bitCount;
    }

    protected int nonzeroBits(int numBytes) {
        int bitCount = 0;
        ByteBuffer wrapper = ByteBuffer.wrap(byteBuffer);
        wrapper.order(ByteOrder.LITTLE_ENDIAN);
        while (numBytes > 8) {
            bitCount += Long.bitCount(wrapper.getLong());
            numBytes -= 8;
        }
        for (int i = 0; i < numBytes; i++) {
            bitCount += Integer.bitCount(wrapper.get() & 0xff);
        }
        return bitCount;
    }
}