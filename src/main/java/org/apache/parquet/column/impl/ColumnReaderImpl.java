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
package org.apache.parquet.column.impl;

//import edu.uchicago.cs.encsel.parquet.EncContext;

import edu.uchicago.cs.db.common.ForwardIterator;
import edu.uchicago.cs.db.common.FullForwardIterator;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.RequiresPreviousReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeNameConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Predicate;

import static java.lang.String.format;
import static org.apache.parquet.Log.DEBUG;
import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.column.ValuesType.*;

/**
 * ColumnReader implementation
 *
 * @author Julien Le Dem
 * @author Modified by Chunwei Liu to support page skippedInPageReader
 */
public class ColumnReaderImpl implements ColumnReader {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.parquet.column.impl.ColumnReaderImpl.class);

    /**
     * binds the lower level page decoder to the record converter materializing the records
     *
     * @author Julien Le Dem
     */
    private static abstract class Binding {

        /**
         * read one value from the underlying page
         */
        abstract void read();

        /**
         * skip one value from the underlying page
         */
        abstract void skip();

        /**
         * Skip n values from the underlying page
         *
         * @param n
         */
        abstract void skip(long n);

        /**
         * write current value to converter
         */
        abstract void writeValue();

        /**
         * @return current value
         */
        public int getDictionaryId() {
            throw new UnsupportedOperationException();
        }

        /**
         * @return current value
         */
        public int getInteger() {
            throw new UnsupportedOperationException();
        }

        /**
         * @return current value
         */
        public boolean getBoolean() {
            throw new UnsupportedOperationException();
        }

        /**
         * @return current value
         */
        public long getLong() {
            throw new UnsupportedOperationException();
        }

        /**
         * @return current value
         */
        public Binary getBinary() {
            throw new UnsupportedOperationException();
        }

        /**
         * @return current value
         */
        public float getFloat() {
            throw new UnsupportedOperationException();
        }

        /**
         * @return current value
         */
        public double getDouble() {
            throw new UnsupportedOperationException();
        }
    }

    private final ParsedVersion writerVersion;
    private final ColumnDescriptor path;
    private final long totalValueCount;
    private final PageReader pageReader;
    private final Dictionary dictionary;

    private IntIterator repetitionLevelColumn;
    private IntIterator definitionLevelColumn;
    protected ValuesReader dataColumn;
    private Encoding currentEncoding;

    private int repetitionLevel;
    private int definitionLevel;
    private int dictionaryId;

    private long endOfPageValueCount;

    private long readValues = 0;
    private long validValues = 0;
    private long readDatas = 0;

    private int pageValueCount = 0;

    private long toSkip = 0;

    private Predicate<Statistics<?>> pageFilter;
    /**
     * Marks the valid row position
     */
    private ForwardIterator rowFilter;

    private final PrimitiveConverter converter;
    private Binding binding;

    // this is needed because we will attempt to read the value twice when filtering
    // TODO: rework that
    private boolean valueRead;

    private void bindToDictionary(final Dictionary dictionary) {
        binding =
                new Binding() {
                    void read() {
                        dictionaryId = dataColumn.readValueDictionaryId();
                    }

                    public void skip() {
                        dataColumn.skip();
                    }

                    public void skip(long n) {
                        dataColumn.skip(n);
                    }

                    public int getDictionaryId() {
                        return dictionaryId;
                    }

                    void writeValue() {
                        //System.out.println(dictionaryId);
                        converter.addInt(dictionaryId);
                    }

                    public int getInteger() {
                        return dictionary.decodeToInt(dictionaryId);
                    }

                    public boolean getBoolean() {
                        return dictionary.decodeToBoolean(dictionaryId);
                    }

                    public long getLong() {
                        return dictionary.decodeToLong(dictionaryId);
                    }

                    public Binary getBinary() {
                        return dictionary.decodeToBinary(dictionaryId);
                    }

                    public float getFloat() {
                        return dictionary.decodeToFloat(dictionaryId);
                    }

                    public double getDouble() {
                        return dictionary.decodeToDouble(dictionaryId);
                    }
                };
    }

    private void bind(PrimitiveTypeName type) {
        binding = type.convert(new PrimitiveTypeNameConverter<Binding, RuntimeException>() {
            @Override
            public Binding convertFLOAT(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                return new Binding() {
                    float current;

                    void read() {
                        current = dataColumn.readFloat();
                    }

                    public void skip() {
                        current = 0;
                        dataColumn.skip();
                    }

                    public void skip(long n) {
                        current = 0;
                        dataColumn.skip(n);
                    }

                    public float getFloat() {
                        return current;
                    }

                    void writeValue() {
                        converter.addFloat(current);
                    }
                };
            }

            @Override
            public Binding convertDOUBLE(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                return new Binding() {
                    double current;

                    void read() {
                        current = dataColumn.readDouble();
                    }

                    public void skip() {
                        current = 0;
                        dataColumn.skip();
                    }

                    public void skip(long n) {
                        current = 0;
                        dataColumn.skip(n);
                    }

                    public double getDouble() {
                        return current;
                    }

                    void writeValue() {
                        converter.addDouble(current);
                    }
                };
            }

            @Override
            public Binding convertINT32(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                return new Binding() {
                    int current;

                    void read() {
                        current = dataColumn.readInteger();
                    }

                    public void skip() {
                        current = 0;
                        dataColumn.skip();
                    }

                    public void skip(long n) {
                        current = 0;
                        dataColumn.skip(n);
                    }

                    @Override
                    public int getInteger() {
                        return current;
                    }

                    void writeValue() {
                        System.out.println(current);
                        converter.addInt(current);
                    }
                };
            }

            @Override
            public Binding convertINT64(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                return new Binding() {
                    long current;

                    void read() {
                        current = dataColumn.readLong();
                    }

                    public void skip() {
                        current = 0;
                        dataColumn.skip();
                    }

                    public void skip(long n) {
                        current = 0;
                        dataColumn.skip(n);
                    }

                    @Override
                    public long getLong() {
                        return current;
                    }

                    void writeValue() {
                        converter.addLong(current);
                    }
                };
            }

            @Override
            public Binding convertINT96(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                return this.convertBINARY(primitiveTypeName);
            }

            @Override
            public Binding convertFIXED_LEN_BYTE_ARRAY(
                    PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                return this.convertBINARY(primitiveTypeName);
            }

            @Override
            public Binding convertBOOLEAN(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                return new Binding() {
                    boolean current;

                    void read() {
                        current = dataColumn.readBoolean();
                    }

                    public void skip() {
                        current = false;
                        dataColumn.skip();
                    }

                    public void skip(long n) {
                        current = false;
                        dataColumn.skip(n);
                    }

                    @Override
                    public boolean getBoolean() {
                        return current;
                    }

                    void writeValue() {
                        converter.addBoolean(current);
                    }
                };
            }

            @Override
            public Binding convertBINARY(PrimitiveTypeName primitiveTypeName) throws RuntimeException {
                return new Binding() {
                    Binary current;

                    void read() {
                        current = dataColumn.readBytes();
                    }

                    public void skip() {
                        current = null;
                        dataColumn.skip();
                    }

                    public void skip(long n) {
                        current = null;
                        dataColumn.skip(n);
                    }

                    @Override
                    public Binary getBinary() {
                        return current;
                    }

                    void writeValue() {
                        converter.addBinary(current);
                    }
                };
            }
        });
    }

    public ColumnReaderImpl(ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter,
                            ParsedVersion writerVersion, Predicate<Statistics<?>> pageFilter, ForwardIterator rowFilter) {
        this.path = checkNotNull(path, "path");
        this.pageReader = checkNotNull(pageReader, "pageReader");
        this.converter = checkNotNull(converter, "converter");
        this.writerVersion = writerVersion;
        this.pageFilter = pageFilter;
        this.rowFilter = rowFilter;

        DictionaryPage dictionaryPage = null;
        dictionaryPage = pageReader.readDictionaryPage();

        if (dictionaryPage != null) {
            try {
                this.dictionary = dictionaryPage.getEncoding().initDictionary(path, dictionaryPage);
                if (converter.hasDictionarySupport()) {
                    converter.setDictionary(dictionary);
                }
            } catch (IOException e) {
                throw new ParquetDecodingException("could not decode the dictionary for " + path, e);
            }
        } else {
            this.dictionary = null;
        }
        this.totalValueCount = pageReader.getTotalValueCount();
        if (totalValueCount <= 0) {
            throw new ParquetDecodingException("totalValueCount '" + totalValueCount + "' <= 0");
        }
        consume();
    }

    /**
     * creates a reader for triplets
     *
     * @param path       the descriptor for the corresponding column
     * @param pageReader the underlying store to read from
     */
    public ColumnReaderImpl(ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter,
                            ParsedVersion writerVersion) {
        this(path, pageReader, converter, writerVersion, stat -> true, new FullForwardIterator());
    }

    public Dictionary getDictionary() {
        return dictionary;
    }

    private boolean isFullyConsumed() {
        return readValues >= totalValueCount;
    }

    /**
     * {@inheritDoc}
     *
     * @see ColumnReader#writeCurrentValueToConverter()
     */
    @Override
    public void writeCurrentValueToConverter() {
        readValue();
        this.binding.writeValue();
    }

    @Override
    public int getCurrentValueDictionaryID() {
        readValue();
        return binding.getDictionaryId();
    }

    /**
     * {@inheritDoc}
     *
     * @see ColumnReader#getInteger()
     */
    @Override
    public int getInteger() {
        readValue();
        return this.binding.getInteger();
    }

    /**
     * {@inheritDoc}
     *
     * @see ColumnReader#getBoolean()
     */
    @Override
    public boolean getBoolean() {
        readValue();
        return this.binding.getBoolean();
    }

    /**
     * {@inheritDoc}
     *
     * @see ColumnReader#getLong()
     */
    @Override
    public long getLong() {
        readValue();
        return this.binding.getLong();
    }

    /**
     * {@inheritDoc}
     *
     * @see ColumnReader#getBinary()
     */
    @Override
    public Binary getBinary() {
        readValue();
        return this.binding.getBinary();
    }

    /**
     * {@inheritDoc}
     *
     * @see ColumnReader#getFloat()
     */
    @Override
    public float getFloat() {
        readValue();
        return this.binding.getFloat();
    }

    /**
     * {@inheritDoc}
     *
     * @see ColumnReader#getDouble()
     */
    @Override
    public double getDouble() {
        readValue();
        return this.binding.getDouble();
    }

    /**
     * {@inheritDoc}
     *
     * @see ColumnReader#getCurrentRepetitionLevel()
     */
    @Override
    public int getCurrentRepetitionLevel() {
        return repetitionLevel;
    }

    /**
     * {@inheritDoc}
     *
     * @see ColumnReader#getDescriptor()
     */
    @Override
    public ColumnDescriptor getDescriptor() {
        return path;
    }

    /**
     * Reads the value into the binding.
     */
    public void readValue() {
        try {
            if (!valueRead) {
                binding.read();
                readDatas++;
                valueRead = true;
            }
        } catch (RuntimeException e) {
            if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, currentEncoding) &&
                    e instanceof ArrayIndexOutOfBoundsException) {
                // this is probably PARQUET-246, which may happen if reading data with
                // MR because this can't be detected without reading all footers
                throw new ParquetDecodingException("Read failure possibly due to " +
                        "PARQUET-246: try setting parquet.split.files to false",
                        new ParquetDecodingException(
                                format("Can't read value in column %s at value %d out of %d, " +
                                                "%d out of %d in currentPage. repetition level: " +
                                                "%d, definition level: %d",
                                        path, readValues, totalValueCount,
                                        readValues - (endOfPageValueCount - pageValueCount),
                                        pageValueCount, repetitionLevel, definitionLevel),
                                e));
            }
            throw new ParquetDecodingException(
                    format("Can't read value in column %s at value %d out of %d, " +
                                    "%d out of %d in currentPage. repetition level: " +
                                    "%d, definition level: %d",
                            path, readValues, totalValueCount,
                            readValues - (endOfPageValueCount - pageValueCount),
                            pageValueCount, repetitionLevel, definitionLevel),
                    e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @see ColumnReader#skip()
     */
    @Override
    public void skip() {
        if (!valueRead) {
            binding.skip();
            readDatas++;
            valueRead = true;
        }
    }


    /**
     * {@inheritDoc}
     *
     * @see ColumnReader#getCurrentDefinitionLevel()
     */
    @Override
    public int getCurrentDefinitionLevel() {
        return definitionLevel;
    }

    // TODO: change the logic around read() to not tie together reading from the 3 columns
    private void readRepetitionAndDefinitionLevels() {
//        repetitionLevel = repetitionLevelColumn.nextInt();
        if (this.optionalMode)
            definitionLevel = definitionLevelColumn.nextInt();
        validValues += (definitionLevel == getDescriptor().getMaxDefinitionLevel()) ? 1 : 0;
        ++readValues;
    }

    private long skipDefinitionAndRepetitionLevels(long numSkip) {
        // Skip and count the number of non-zeros in def value ( for null skipping)
        long numValid = numSkip;
        if (this.optionalMode) {
            numValid = definitionLevelColumn.skipWithCount(numSkip);
        } else {
            definitionLevelColumn.skip(numSkip);
        }
        readValues += numSkip;
        validValues += numValid;
        return numValid;
    }

    private void checkRead() {
        if (isPageFullyConsumed()) {
            readPage();
            // It is possible that all remaining pages can be skipped, need to check again
            if (isFullyConsumed()) {
                if (DEBUG)
                    LOG.debug("end reached");
                return;
            }
        }

        // Move to the next valid position marked by row filter
        if (toSkip > 0) {
            consumeTo(readValues + toSkip);
            toSkip = 0;
        } else {
            long currentPos = readValues;
            rowFilter.startfrom(currentPos);
            if (rowFilter.hasNext()) {
                long nextPos = rowFilter.nextLong();
                if (nextPos != currentPos) {
                    consumeTo(nextPos);
                } else {
                    // Simply read the current value
                    readRepetitionAndDefinitionLevels();
                    valueRead = false;
                }
            } else {
                readValues = totalValueCount + 1;
                return;
            }
        }
    }

    public long getReadValue() {
        return this.readValues;
    }

    public long getCurPos() {
        return this.readValues - 1;
    }

    public int getPageValueCount() {
        return this.pageValueCount;
    }

    /**
     * This method looks for the next valid page satisfying both page filter and row filter.
     * After it returns, the toSkip property will be updated to reflect the next number to read.
     */
    private void readPage() {
        if (DEBUG)
            LOG.debug("loading page");

        if (isFullyConsumed()) {
            readValues++;
            return;
        }
        DataPage.Visitor<Boolean> acceptPage = new DataPage.Visitor<Boolean>() {
            @Override
            public Boolean visit(DataPageV1 dataPageV1) {
                return pageFilter.test(dataPageV1.getStatistics());
            }

            @Override
            public Boolean visit(DataPageV2 dataPageV2) {
                return pageFilter.test(dataPageV2.getStatistics());
            }
        };
        // If the page fails filter test, skip it
        // If the page is not included in the row filter, skip it

        // First use the toSkip to consume pages
        DataPage page = pageReader.readPage();

        while (page != null && toSkip >= page.getValueCount()) {
            this.readValues += page.getValueCount();
            this.validValues = this.readValues;
            this.readDatas = this.readValues;
            this.endOfPageValueCount += page.getValueCount();
            toSkip -= page.getValueCount();
            page = pageReader.readPage();
        }
        // Check pageFilter for next valid page
        while (page != null && !page.accept(acceptPage)) {
            // Need recompute
            toSkip = -1;
            this.readValues += page.getValueCount();
            this.validValues = this.readValues;
            this.readDatas = this.readValues;
            this.endOfPageValueCount += page.getValueCount();
            page = pageReader.readPage();
        }

        if (isFullyConsumed()) {
            if (DEBUG)
                LOG.debug("end reached");
            readValues++;
            repetitionLevel = 0; // the next repetition level
            return;
        }

        this.endOfPageValueCount += page.getValueCount();

        page.accept(new DataPage.Visitor<Void>() {
            @Override
            public Void visit(DataPageV1 dataPageV1) {
                readPageV1(dataPageV1);
                return null;
            }

            @Override
            public Void visit(DataPageV2 dataPageV2) {
                readPageV2(dataPageV2);
                return null;
            }
        });

    }

    private void initDataReader(Encoding dataEncoding, ByteBuffer bytes, int offset, int valueCount) {
        ValuesReader previousReader = this.dataColumn;
        this.currentEncoding = dataEncoding;
        this.pageValueCount = valueCount;
        this.endOfPageValueCount = readValues + pageValueCount;

        if (dataEncoding.usesDictionary()) {
            if (dictionary == null) {
                throw new ParquetDecodingException(
                        "could not read page in col " + path + " as the dictionary was missing for encoding " + dataEncoding);
            }
            this.dataColumn = dataEncoding.getDictionaryBasedValuesReader(path, VALUES, dictionary);
        } else {
            this.dataColumn = dataEncoding.getValuesReader(path, VALUES);
        }
        //if (dataEncoding.usesDictionary() && converter.hasDictionarySupport()) {
        if (dataEncoding.usesDictionary()) {
            //System.out.println("use dictionary value reader");
            bindToDictionary(dictionary);
        } else {
            //System.out.println("do not use dictionary value reader");
            bind(path.getType());
        }
        try {
            dataColumn.initFromPage(pageValueCount, bytes, offset);
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page in col " + path, e);
        }

        if (CorruptDeltaByteArrays.requiresSequentialReads(writerVersion, dataEncoding) &&
                previousReader != null && previousReader instanceof RequiresPreviousReader) {
            // previous reader can only be set if reading sequentially
            ((RequiresPreviousReader) dataColumn).setPreviousReader(previousReader);
        }
    }

    private void readPageV1(DataPageV1 page) {
        ValuesReader rlReader = page.getRlEncoding().getValuesReader(path, REPETITION_LEVEL);
        ValuesReader dlReader = page.getDlEncoding().getValuesReader(path, DEFINITION_LEVEL);
        this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
        this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
        try {
            ByteBuffer bytes = page.getBytes().toByteBuffer();
            if (DEBUG) {
                LOG.debug("page size {} bytes and {} records", bytes.remaining(), pageValueCount);
                LOG.debug("reading repetition levels at 0");
            }
            rlReader.initFromPage(pageValueCount, bytes, 0);
            int next = rlReader.getNextOffset();
            if (DEBUG)
                LOG.debug("reading definition levels at {}", next);
            dlReader.initFromPage(pageValueCount, bytes, next);
            next = dlReader.getNextOffset();
            if (DEBUG)
                LOG.debug("reading data at {}", next);
            initDataReader(page.getValueEncoding(), bytes, next, page.getValueCount());
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + path, e);
        }
    }

    private void readPageV2(DataPageV2 page) {
        this.repetitionLevelColumn = newRLEIterator(path.getMaxRepetitionLevel(), page.getRepetitionLevels());
        this.definitionLevelColumn = newRLEIterator(path.getMaxDefinitionLevel(), page.getDefinitionLevels());
        try {
            if (DEBUG)
                LOG.debug("page data size {} bytes and {} records", page.getData().size(), pageValueCount);
            initDataReader(page.getDataEncoding(), page.getData().toByteBuffer(), 0, page.getValueCount());
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read page " + page + " in col " + path, e);
        }
    }

    private IntIterator newRLEIterator(int maxLevel, BytesInput bytes) {
        try {
            if (maxLevel == 0) {
                return new NullIntIterator();
            }
            return new RLEIntIterator(
                    new RunLengthBitPackingHybridDecoder(
                            BytesUtils.getWidthFromMaxInt(maxLevel),
                            bytes.toInputStream()));
        } catch (IOException e) {
            throw new ParquetDecodingException("could not read levels in page for col " + path, e);
        }
    }

    private boolean isPageFullyConsumed() {
        return readValues >= endOfPageValueCount;
    }

    /**
     * {@inheritDoc}
     *
     * @see ColumnReader#consume()
     */
    @Override
    public void consume() {
        checkRead();
        valueRead = false;
    }

    public void consumeTo(long location) {
        if (location <= readValues - 1) {
            return;
        }
        if (location >= totalValueCount)
            if (DEBUG)
                LOG.debug("Skip too much, reach the end!");
        if (location < endOfPageValueCount) {
            // Enough records left in current page
            consumeInPage(location - readValues + 1);
        } else {
            // Crossing page boundary skipping
            // Discard the remaining values in current page
            toSkip = location - endOfPageValueCount;
            readValues = endOfPageValueCount;
            // When a new page is read, these values are reset
            validValues = readValues;
            readDatas = readValues;
            // After this call, we are guaranteed to come to a valid page or to the end
            checkRead();
        }
        valueRead = false;
    }

    protected void consumeInPage(long numConsume) {
        if (0 == numConsume) {
            return;
        }
        long numValid = skipDefinitionAndRepetitionLevels(numConsume - 1);

        if (validValues > readDatas) {
            binding.skip(validValues - readDatas);
            readDatas = validValues;
        }
        readRepetitionAndDefinitionLevels();
    }

    /**
     * {@inheritDoc}
     *
     * @see ColumnReader#getTotalValueCount()
     */
    @Override
    public long getTotalValueCount() {
        return totalValueCount;
    }

    static abstract class IntIterator {
        abstract int nextInt();

        void skip(long count) {
            for (long i = 0; i < count; i++) {
                nextInt();
            }
        }

        /**
         * @param count
         * @return the number of non-zeros
         */
        long skipWithCount(long count) {
            long counter = 0;
            for (long i = 0; i < count; i++) {
                counter += nextInt() == 0 ? 0 : 1;
            }
            return count;
        }
    }

    static class ValuesReaderIntIterator extends IntIterator {
        ValuesReader delegate;

        public ValuesReaderIntIterator(ValuesReader delegate) {
            super();
            this.delegate = delegate;
        }

        @Override
        int nextInt() {
            return delegate.readInteger();
        }

        @Override
        void skip(long count) {
            delegate.skip(count);
        }

        @Override
        long skipWithCount(long count) {
            return delegate.skipWithCount(count);
        }
    }

    static class RLEIntIterator extends IntIterator {
        RunLengthBitPackingHybridDecoder delegate;

        public RLEIntIterator(RunLengthBitPackingHybridDecoder delegate) {
            this.delegate = delegate;
        }

        @Override
        int nextInt() {
            try {
                return delegate.readInt();
            } catch (IOException e) {
                throw new ParquetDecodingException(e);
            }
        }

        @Override
        void skip(long count) {
            try {
                delegate.skip(count);
            } catch (IOException e) {
                throw new ParquetDecodingException(e);
            }
        }

        @Override
        long skipWithCount(long count) {
            try {
                return delegate.skipWithCount(count);
            } catch (IOException e) {
                throw new ParquetDecodingException(e);
            }
        }
    }

    private static final class NullIntIterator extends IntIterator {
        @Override
        int nextInt() {
            return 0;
        }

    }

    private boolean optionalMode = true;

    public void useOptionalMode(boolean use) {
        this.optionalMode = use;
    }
}
