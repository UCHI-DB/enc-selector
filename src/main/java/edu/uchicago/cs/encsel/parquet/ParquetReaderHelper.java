package edu.uchicago.cs.encsel.parquet;

import edu.uchicago.cs.encsel.query.tpch.TPCHSchema;
import edu.uchicago.cs.encsel.util.perf.ProfileBean;
import edu.uchicago.cs.encsel.util.perf.Profiler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntMap;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.io.File;

import static org.apache.parquet.column.Encoding.PLAIN;

public class ParquetReaderHelper {
    static private ParquetProperties parquetProperties = ParquetProperties.builder().build();
    public static void read(URI file, ReaderProcessor processor) throws IOException, VersionParser.VersionParseException {
        Configuration conf = new Configuration();
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);
        List<FileStatus> statuses = Arrays.asList(fs.listStatus(path, HiddenFileFilter.INSTANCE));
        List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);
        if (footers.isEmpty()) {
            return;
        }
        for (Footer footer : footers) {
            processor.processFooter(footer);
            ParquetReaderHelper.getGlobalDict(footer);
            //System.out.println(footer.getParquetMetadata().getBlocks().get(0).getColumns().get(10).getStatistics());
            VersionParser.ParsedVersion version = VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());

            ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile(), footer.getParquetMetadata());
            PageReadStore rowGroup = null;
            int blockCounter = 0;
            List<ColumnDescriptor> cols = footer.getParquetMetadata().getFileMetaData().getSchema().getColumns();
            while ((rowGroup = fileReader.readNextRowGroup()) != null) {
                BlockMetaData blockMeta = footer.getParquetMetadata().getBlocks().get(blockCounter);
                processor.processRowGroup(version, blockMeta, rowGroup);
                blockCounter++;
            }
        }
    }

    public static ProfileBean profile(URI file, ReaderProcessor processor) throws IOException, VersionParser.VersionParseException {
        Configuration conf = new Configuration();
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);
        List<FileStatus> statuses = Arrays.asList(fs.listStatus(path, HiddenFileFilter.INSTANCE));
        List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);
        if (footers.isEmpty()) {
            return null;
        }

        Profiler profiler = new Profiler();
        for (Footer footer : footers) {
            profiler.mark();
            processor.processFooter(footer);
            profiler.pause();
            ParquetReaderHelper.getGlobalDict(footer);
            //System.out.println(footer.getParquetMetadata().getBlocks().get(0).getColumns().get(10).getStatistics());
            VersionParser.ParsedVersion version = VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());
            ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile(), footer.getParquetMetadata());
            PageReadStore rowGroup = null;
            int blockCounter = 0;
            List<ColumnDescriptor> cols = footer.getParquetMetadata().getFileMetaData().getSchema().getColumns();
            while ((rowGroup = fileReader.readNextRowGroup()) != null) {
                BlockMetaData blockMeta = footer.getParquetMetadata().getBlocks().get(blockCounter);
                profiler.mark();
                processor.processRowGroup(version, blockMeta, rowGroup);
                profiler.pause();
                blockCounter++;
            }
        }
        return profiler.stop();
    }

    public static void getGlobalDict (Footer footer){
        Map<String, String> keyValueMeta = footer.getParquetMetadata().getFileMetaData().getKeyValueMetaData();
        List<ColumnDescriptor> cols = footer.getParquetMetadata().getFileMetaData().getSchema().getColumns();
        for (ColumnDescriptor col : cols) {
            if (keyValueMeta.containsKey(col.toString()+".DICT")) {
                //System.out.println(keyValueMeta.get(col.toString()+".DICT"));
                EncContext.dictPage.get().put(col.toString(), buildGlobalDict(keyValueMeta.get(col.toString()+".DICT"), col));
            }
        }
    }

    public static DictionaryPage buildGlobalDict(String globDict, ColumnDescriptor col){
        DictionaryPage page = null;
        String[] dictValues = globDict.split("\\|");
        if (dictValues.length > 0) {
            // return a dictionary only if we actually used it
            Object2IntMap dictionaryContent = null;
            switch (col.getType()) {
                case BINARY:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Binary>();
                    PlainValuesWriter dictionaryEncoder = new PlainValuesWriter(dictValues.length, parquetProperties.getDictionaryPageSizeThreshold(), parquetProperties.getAllocator());
                    for (int i = 0; i < dictValues.length; i++) {
                        Binary entry = Binary.fromString(dictValues[i]);
                        dictionaryEncoder.writeBytes(entry);
                        dictionaryContent.put(entry, i);
                    }
                    EncContext.globalDict.get().put(col.toString(), dictionaryContent);
                    return dictPage(dictionaryEncoder, dictValues.length);

                case FIXED_LEN_BYTE_ARRAY:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Binary>();
                    FixedLenByteArrayPlainValuesWriter fixedDictionaryEncoder = new FixedLenByteArrayPlainValuesWriter(col.getTypeLength(), dictValues.length, parquetProperties.getDictionaryPageSizeThreshold(), parquetProperties.getAllocator());
                    for (int i = 0; i < dictValues.length; i++) {
                        Binary entry = Binary.fromString(dictValues[i]);
                        fixedDictionaryEncoder.writeBytes(entry);
                        dictionaryContent.put(entry, i);
                    }
                    EncContext.globalDict.get().put(col.toString(), dictionaryContent);
                    return dictPage(fixedDictionaryEncoder, dictValues.length);

                case INT96:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Binary>();
                    FixedLenByteArrayPlainValuesWriter i96DictionaryEncoder = new FixedLenByteArrayPlainValuesWriter(col.getTypeLength(), dictValues.length, parquetProperties.getDictionaryPageSizeThreshold(), parquetProperties.getAllocator());
                    for (int i = 0; i < dictValues.length; i++) {
                        Binary entry = Binary.fromString(dictValues[i]);
                        i96DictionaryEncoder.writeBytes(entry);
                        dictionaryContent.put(entry, i);
                    }
                    EncContext.globalDict.get().put(col.toString(), dictionaryContent);
                    return dictPage(i96DictionaryEncoder, dictValues.length);

                case INT64:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Long>();
                    PlainValuesWriter longDictionaryEncoder = new PlainValuesWriter(dictValues.length, parquetProperties.getDictionaryPageSizeThreshold(), parquetProperties.getAllocator());
                    for (int i = 0; i < dictValues.length; i++) {
                        Long entry = Long.parseLong(dictValues[i]);
                        longDictionaryEncoder.writeLong(entry);
                        dictionaryContent.put(entry, i);
                    }
                    EncContext.globalDict.get().put(col.toString(), dictionaryContent);
                    return dictPage(longDictionaryEncoder, dictValues.length);

                case DOUBLE:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Double>();
                    PlainValuesWriter doubleDictionaryEncoder = new PlainValuesWriter(dictValues.length, parquetProperties.getDictionaryPageSizeThreshold(), parquetProperties.getAllocator());
                    for (int i = 0; i < dictValues.length; i++) {
                        Double entry = Double.parseDouble(dictValues[i]);
                        doubleDictionaryEncoder.writeDouble(entry);
                        dictionaryContent.put(entry, i);
                    }
                    EncContext.globalDict.get().put(col.toString(), dictionaryContent);
                    return dictPage(doubleDictionaryEncoder, dictValues.length);

                case INT32:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Integer>();
                    PlainValuesWriter intDictionaryEncoder = new PlainValuesWriter(dictValues.length, parquetProperties.getDictionaryPageSizeThreshold(), parquetProperties.getAllocator());
                    for (int i = 0; i < dictValues.length; i++) {
                        Integer entry = Integer.parseInt(dictValues[i]);
                        intDictionaryEncoder.writeInteger(entry);
                        dictionaryContent.put(entry, i);
                    }
                    EncContext.globalDict.get().put(col.toString(), dictionaryContent);
                    return dictPage(intDictionaryEncoder, dictValues.length);

                case FLOAT:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Float>();
                    PlainValuesWriter floatDictionaryEncoder = new PlainValuesWriter(dictValues.length, parquetProperties.getDictionaryPageSizeThreshold(), parquetProperties.getAllocator());
                    for (int i = 0; i < dictValues.length; i++) {
                        Float entry = Float.parseFloat(dictValues[i]);
                        floatDictionaryEncoder.writeFloat(entry);
                        dictionaryContent.put(entry, i);
                    }
                    EncContext.globalDict.get().put(col.toString(), dictionaryContent);
                    return dictPage(floatDictionaryEncoder, dictValues.length);

                default:
                    throw new ParquetDecodingException("Dictionary encoding not supported for type: " + col.getType());
            }
        }
        return null;
    }

    static protected DictionaryPage dictPage(ValuesWriter dictPageWriter, int lastUsedDictionarySize) {
        DictionaryPage ret = new DictionaryPage(dictPageWriter.getBytes(), lastUsedDictionarySize, getEncodingForDictionaryPage());
        dictPageWriter.close();
        return ret;
    }

    static private Encoding getEncodingForDictionaryPage() {
        return PLAIN;
    }

    public static long getColSize(URI file, int col) throws IOException, VersionParser.VersionParseException {
        Configuration conf = new Configuration();
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);
        List<FileStatus> statuses = Arrays.asList(fs.listStatus(path, HiddenFileFilter.INSTANCE));
        List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);
        if (footers.isEmpty()) {
            return -1;
        }
        String colEncoding = null;
        long colsize = 0;
        for (Footer footer : footers) {
            List<BlockMetaData> blockMetas = footer.getParquetMetadata().getBlocks();
            for (BlockMetaData block : blockMetas){
                colsize += block.getColumns().get(col).getTotalSize();
                colEncoding =block.getColumns().get(col).toString();
            }
        }
        System.out.println(file);
        System.out.println("Filesize:" + new File(file).length());
        System.out.println(colEncoding);
        System.out.println("col " + col + " size:"+colsize);

        return colsize;
    }

    public static ProfileBean filterProfile(URI file, FilterCompat.Filter filter, ReaderProcessor processor) throws IOException, VersionParser.VersionParseException {
        Configuration conf = new Configuration();
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);
        List<FileStatus> statuses = Arrays.asList(fs.listStatus(path, HiddenFileFilter.INSTANCE));
        List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);
        if (footers.isEmpty()) {
            return null;
        }

        Profiler profiler = new Profiler();
        for (Footer footer : footers) {
            profiler.mark();
            processor.processFooter(footer);
            profiler.pause();
            ParquetReaderHelper.getGlobalDict(footer);
            //System.out.println(footer.getParquetMetadata().getBlocks().get(0).getColumns().get(10).getStatistics());
            VersionParser.ParsedVersion version = VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());

            ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile(), footer.getParquetMetadata());
            fileReader.filterOutRow(filter);
            PageReadStore rowGroup = null;
            int blockCounter = 0;
            List<ColumnDescriptor> cols = footer.getParquetMetadata().getFileMetaData().getSchema().getColumns();
            while ((rowGroup = fileReader.readNextRowGroup()) != null) {
                BlockMetaData blockMeta = footer.getParquetMetadata().getBlocks().get(blockCounter);
                profiler.mark();
                processor.processRowGroup(version, blockMeta, rowGroup);
                profiler.pause();
                blockCounter++;
            }
        }
        return profiler.stop();
    }
}
