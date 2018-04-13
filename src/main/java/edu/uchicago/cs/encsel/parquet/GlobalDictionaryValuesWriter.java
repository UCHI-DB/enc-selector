/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *
 *
 * Contributors:
 *     Chunwei Liu - initial API and implementation
 */

package edu.uchicago.cs.encsel.parquet;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.IntList;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.it.unimi.dsi.fastutil.doubles.Double2IntLinkedOpenHashMap;
import org.apache.parquet.it.unimi.dsi.fastutil.doubles.Double2IntMap;
import org.apache.parquet.it.unimi.dsi.fastutil.doubles.DoubleIterator;
import org.apache.parquet.it.unimi.dsi.fastutil.floats.Float2IntLinkedOpenHashMap;
import org.apache.parquet.it.unimi.dsi.fastutil.floats.Float2IntMap;
import org.apache.parquet.it.unimi.dsi.fastutil.floats.FloatIterator;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.Int2IntLinkedOpenHashMap;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.Int2IntMap;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.parquet.it.unimi.dsi.fastutil.longs.Long2IntLinkedOpenHashMap;
import org.apache.parquet.it.unimi.dsi.fastutil.longs.Long2IntMap;
import org.apache.parquet.it.unimi.dsi.fastutil.longs.LongIterator;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Iterator;

import static org.apache.parquet.bytes.BytesInput.concat;

public abstract class GlobalDictionaryValuesWriter extends DictionaryValuesWriter {
    /* indicating if global dictionary is enabled*/
    protected Object2IntMap globalDict = null;

    public void setGlobalDict(Object2IntMap globalDict) {
        this.globalDict = globalDict;
    }

    public boolean IsGlobalDictEnabled(){
        return this.globalDict != null;
    }

    public GlobalDictionaryValuesWriter(int maxDictionaryByteSize, Encoding encodingForDataPage, Encoding encodingForDictionaryPage, ByteBufferAllocator allocator) {
        super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
    }

    /**
     *
     */
    public static class PlainBinaryGlobalDictionaryValuesWriter extends GlobalDictionaryValuesWriter {

        /* type specific dictionary content */
        protected Object2IntMap<Binary> binaryDictionaryContent = new Object2IntLinkedOpenHashMap<Binary>();
        protected Object2IntMap<Binary> globalDict;
        /**
         * @param maxDictionaryByteSize
         */
        public PlainBinaryGlobalDictionaryValuesWriter(int maxDictionaryByteSize, Encoding encodingForDataPage, Encoding encodingForDictionaryPage, ByteBufferAllocator allocator) {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
            binaryDictionaryContent.defaultReturnValue(-1);
        }

        @Override
        public void setGlobalDict(Object2IntMap globalDict) {
            this.globalDict = globalDict;
        }

        @Override
        public void writeBytes(Binary v) {
            //System.out.println("global:"+ v.toString() + " " +globalDict.get(v));
            int id = binaryDictionaryContent.getInt(v);
            //System.out.println("Local:"+ v.toStringUsingUTF8() + " " +id);
            if (id == -1) {
                id = binaryDictionaryContent.size();
                binaryDictionaryContent.put(v.copy(), id);
                // length as int (4 bytes) + actual bytes
                dictionaryByteSize += 4 + v.length();
            }

            encodedValues.add(globalDict.get(v));
        }

        @Override
        public DictionaryPage toDictPageAndClose() {
            if (lastUsedDictionarySize > 0) {
                // return a dictionary only if we actually used it
                PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);
                Iterator<Binary> binaryIterator = binaryDictionaryContent.keySet().iterator();
                // write only the part of the dict that we used
                for (int i = 0; i < lastUsedDictionarySize; i++) {
                    Binary entry = binaryIterator.next();
                    dictionaryEncoder.writeBytes(entry);
                }
                return dictPage(dictionaryEncoder);
            }
            return null;
        }

        @Override
        public int getDictionarySize() {
            return binaryDictionaryContent.size();
        }

        @Override
        protected void clearDictionaryContent() {
            binaryDictionaryContent.clear();
        }

        @Override
        public void fallBackDictionaryEncodedData(ValuesWriter writer) {

            System.out.println("fallback");
            //build reverse dictionary
            Binary[] reverseDictionary = new Binary[getDictionarySize()];
            for (Object2IntMap.Entry<Binary> entry : globalDict.object2IntEntrySet()) {
                reverseDictionary[entry.getIntValue()] = entry.getKey();
            }

            //fall back to plain encoding
            IntList.IntIterator iterator = encodedValues.iterator();
            while (iterator.hasNext()) {
                int id = iterator.next();
                writer.writeBytes(reverseDictionary[id]);
            }
        }
    }

    /**
     *
     */
    public static class PlainFixedLenArrayGlobalDictionaryValuesWriter extends PlainBinaryGlobalDictionaryValuesWriter {

        private final int length;

        /**
         * @param maxDictionaryByteSize
         */
        public PlainFixedLenArrayGlobalDictionaryValuesWriter(int maxDictionaryByteSize, int length, Encoding encodingForDataPage, Encoding encodingForDictionaryPage, ByteBufferAllocator allocator) {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
            this.length = length;
        }

        @Override
        public void writeBytes(Binary value) {
            int id = binaryDictionaryContent.getInt(value);
            if (id == -1) {
                id = binaryDictionaryContent.size();
                binaryDictionaryContent.put(value.copy(), id);
                dictionaryByteSize += length;
            }
            encodedValues.add(globalDict.get(value));
        }

        @Override
        public DictionaryPage toDictPageAndClose() {
            if (lastUsedDictionarySize > 0) {
                // return a dictionary only if we actually used it
                FixedLenByteArrayPlainValuesWriter dictionaryEncoder = new FixedLenByteArrayPlainValuesWriter(length, lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);
                Iterator<Binary> binaryIterator = binaryDictionaryContent.keySet().iterator();
                // write only the part of the dict that we used
                for (int i = 0; i < lastUsedDictionarySize; i++) {
                    Binary entry = binaryIterator.next();
                    dictionaryEncoder.writeBytes(entry);
                }
                return dictPage(dictionaryEncoder);
            }
            return null;
        }
    }

    /**
     *
     */
    public static class PlainLongGlobalDictionaryValuesWriter extends GlobalDictionaryValuesWriter {

        /* type specific dictionary content */
        private Long2IntMap longDictionaryContent = new Long2IntLinkedOpenHashMap();
        protected Object2IntMap<Long> globalDict = super.globalDict;

        /**
         * @param maxDictionaryByteSize
         */
        public PlainLongGlobalDictionaryValuesWriter(int maxDictionaryByteSize, Encoding encodingForDataPage, Encoding encodingForDictionaryPage, ByteBufferAllocator allocator) {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
            longDictionaryContent.defaultReturnValue(-1);
        }

        @Override
        public void setGlobalDict(Object2IntMap globalDict) {
            this.globalDict = globalDict;
        }

        @Override
        public void writeLong(long v) {
            int id = longDictionaryContent.get(v);
            if (id == -1) {
                id = longDictionaryContent.size();
                longDictionaryContent.put(v, id);
                dictionaryByteSize += 8;
            }
            encodedValues.add(globalDict.get(v));
        }

        @Override
        public DictionaryPage toDictPageAndClose() {
            if (lastUsedDictionarySize > 0) {
                // return a dictionary only if we actually used it
                PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);
                LongIterator longIterator = longDictionaryContent.keySet().iterator();
                // write only the part of the dict that we used
                for (int i = 0; i < lastUsedDictionarySize; i++) {
                    dictionaryEncoder.writeLong(longIterator.nextLong());
                }
                return dictPage(dictionaryEncoder);
            }
            return null;
        }

        @Override
        public int getDictionarySize() {
            return longDictionaryContent.size();
        }

        @Override
        protected void clearDictionaryContent() {
            longDictionaryContent.clear();
        }

        @Override
        public void fallBackDictionaryEncodedData(ValuesWriter writer) {
            //build reverse dictionary
            long[] reverseDictionary = new long[getDictionarySize()];
            ObjectIterator<Object2IntMap.Entry<Long>> entryIterator = globalDict.object2IntEntrySet().iterator();
            while (entryIterator.hasNext()) {
                Object2IntMap.Entry entry = entryIterator.next();
                reverseDictionary[entry.getIntValue()] = (Long) entry.getKey();
            }

            //fall back to plain encoding
            IntList.IntIterator iterator = encodedValues.iterator();
            while (iterator.hasNext()) {
                int id = iterator.next();
                writer.writeLong(reverseDictionary[id]);
            }
        }
    }

    /**
     *
     */
    public static class PlainDoubleGlobalDictionaryValuesWriter extends GlobalDictionaryValuesWriter {

        /* type specific dictionary content */
        private Double2IntMap doubleDictionaryContent = new Double2IntLinkedOpenHashMap();
        protected Object2IntMap<Double> globalDict = super.globalDict;

        /**
         * @param maxDictionaryByteSize
         */
        public PlainDoubleGlobalDictionaryValuesWriter(int maxDictionaryByteSize, Encoding encodingForDataPage, Encoding encodingForDictionaryPage, ByteBufferAllocator allocator) {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
            doubleDictionaryContent.defaultReturnValue(-1);
        }

        @Override
        public void setGlobalDict(Object2IntMap globalDict) {
            this.globalDict = globalDict;
        }

        @Override
        public void writeDouble(double v) {
            int id = doubleDictionaryContent.get(v);
            if (id == -1) {
                id = doubleDictionaryContent.size();
                doubleDictionaryContent.put(v, id);
                dictionaryByteSize += 8;
            }
            encodedValues.add(globalDict.get(globalDict.get(v)));
        }

        @Override
        public DictionaryPage toDictPageAndClose() {
            if (lastUsedDictionarySize > 0) {
                // return a dictionary only if we actually used it
                PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);
                DoubleIterator doubleIterator = doubleDictionaryContent.keySet().iterator();
                // write only the part of the dict that we used
                for (int i = 0; i < lastUsedDictionarySize; i++) {
                    dictionaryEncoder.writeDouble(doubleIterator.nextDouble());
                }
                return dictPage(dictionaryEncoder);
            }
            return null;
        }

        @Override
        public int getDictionarySize() {
            return doubleDictionaryContent.size();
        }

        @Override
        protected void clearDictionaryContent() {
            doubleDictionaryContent.clear();
        }

        @Override
        public void fallBackDictionaryEncodedData(ValuesWriter writer) {
            //build reverse dictionary
            double[] reverseDictionary = new double[getDictionarySize()];
            ObjectIterator<Object2IntMap.Entry<Double>> entryIterator = globalDict.object2IntEntrySet().iterator();
            while (entryIterator.hasNext()) {
                Object2IntMap.Entry entry = entryIterator.next();
                reverseDictionary[entry.getIntValue()] = (Double)entry.getKey();
            }

            //fall back to plain encoding
            IntList.IntIterator iterator = encodedValues.iterator();
            while (iterator.hasNext()) {
                int id = iterator.next();
                writer.writeDouble(reverseDictionary[id]);
            }
        }
    }

    /**
     *
     */
    public static class PlainIntegerGlobalDictionaryValuesWriter extends GlobalDictionaryValuesWriter {

        /* type specific dictionary content */
        private Int2IntMap intDictionaryContent = new Int2IntLinkedOpenHashMap();
        protected Object2IntMap<Integer> globalDict = super.globalDict;


        /**
         * @param maxDictionaryByteSize
         */
        public PlainIntegerGlobalDictionaryValuesWriter(int maxDictionaryByteSize, Encoding encodingForDataPage, Encoding encodingForDictionaryPage, ByteBufferAllocator allocator) {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
            intDictionaryContent.defaultReturnValue(-1);
        }

        @Override
        public void setGlobalDict(Object2IntMap globalDict) {
            this.globalDict = globalDict;
        }

        @Override
        public void writeInteger(int v) {
            int id = intDictionaryContent.get(v);
            if (id == -1) {
                id = intDictionaryContent.size();
                intDictionaryContent.put(v, id);
                dictionaryByteSize += 4;
            }
            encodedValues.add(globalDict.get(v));
        }

        @Override
        public DictionaryPage toDictPageAndClose() {
            if (lastUsedDictionarySize > 0) {
                // return a dictionary only if we actually used it
                PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);
                IntIterator intIterator = intDictionaryContent.keySet().iterator();
                // write only the part of the dict that we used
                for (int i = 0; i < lastUsedDictionarySize; i++) {
                    dictionaryEncoder.writeInteger(intIterator.nextInt());
                }
                return dictPage(dictionaryEncoder);
            }
            return null;
        }

        @Override
        public int getDictionarySize() {
            return intDictionaryContent.size();
        }

        @Override
        protected void clearDictionaryContent() {
            intDictionaryContent.clear();
        }

        @Override
        public void fallBackDictionaryEncodedData(ValuesWriter writer) {
            //build reverse dictionary
            int[] reverseDictionary = new int[getDictionarySize()];
            ObjectIterator<Object2IntMap.Entry<Integer>> entryIterator = globalDict.object2IntEntrySet().iterator();
            while (entryIterator.hasNext()) {
                Object2IntMap.Entry entry = entryIterator.next();
                reverseDictionary[entry.getIntValue()] = (Integer)entry.getKey();
            }

            //fall back to plain encoding
            IntList.IntIterator iterator = encodedValues.iterator();
            while (iterator.hasNext()) {
                int id = iterator.next();
                writer.writeInteger(reverseDictionary[id]);
            }
        }
    }

    /**
     *
     */
    public static class PlainFloatGlobalDictionaryValuesWriter extends GlobalDictionaryValuesWriter{

        /* type specific dictionary content */
        private Float2IntMap floatDictionaryContent = new Float2IntLinkedOpenHashMap();
        protected Object2IntMap<Float> globalDict = super.globalDict;


        /**
         * @param maxDictionaryByteSize
         */
        public PlainFloatGlobalDictionaryValuesWriter(int maxDictionaryByteSize, Encoding encodingForDataPage, Encoding encodingForDictionaryPage, ByteBufferAllocator allocator) {
            super(maxDictionaryByteSize, encodingForDataPage, encodingForDictionaryPage, allocator);
            floatDictionaryContent.defaultReturnValue(-1);
        }

        @Override
        public void setGlobalDict(Object2IntMap globalDict) {
            this.globalDict = globalDict;
        }

        @Override
        public void writeFloat(float v) {
            int id = floatDictionaryContent.get(v);
            if (id == -1) {
                id = floatDictionaryContent.size();
                floatDictionaryContent.put(v, id);
                dictionaryByteSize += 4;
            }
            encodedValues.add(globalDict.get(v));
        }

        @Override
        public DictionaryPage toDictPageAndClose() {
            if (lastUsedDictionarySize > 0) {
                // return a dictionary only if we actually used it
                PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(lastUsedDictionaryByteSize, maxDictionaryByteSize, allocator);
                FloatIterator floatIterator = floatDictionaryContent.keySet().iterator();
                // write only the part of the dict that we used
                for (int i = 0; i < lastUsedDictionarySize; i++) {
                    dictionaryEncoder.writeFloat(floatIterator.nextFloat());
                }
                return dictPage(dictionaryEncoder);
            }
            return null;
        }

        @Override
        public int getDictionarySize() {
            return floatDictionaryContent.size();
        }

        @Override
        protected void clearDictionaryContent() {
            floatDictionaryContent.clear();
        }

        @Override
        public void fallBackDictionaryEncodedData(ValuesWriter writer) {
            //build reverse dictionary
            float[] reverseDictionary = new float[getDictionarySize()];
            ObjectIterator<Object2IntMap.Entry<Float>> entryIterator = globalDict.object2IntEntrySet().iterator();
            while (entryIterator.hasNext()) {
                Object2IntMap.Entry entry = entryIterator.next();
                reverseDictionary[entry.getIntValue()] = (Float)entry.getKey();
            }

            //fall back to plain encoding
            IntList.IntIterator iterator = encodedValues.iterator();
            while (iterator.hasNext()) {
                int id = iterator.next();
                writer.writeFloat(reverseDictionary[id]);
            }
        }
    }

}
