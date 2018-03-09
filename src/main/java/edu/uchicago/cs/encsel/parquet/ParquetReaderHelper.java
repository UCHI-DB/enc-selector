package edu.uchicago.cs.encsel.parquet;

import edu.uchicago.cs.encsel.util.perf.ProfileBean;
import edu.uchicago.cs.encsel.util.perf.Profiler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HiddenFileFilter;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

public class ParquetReaderHelper {
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
}
