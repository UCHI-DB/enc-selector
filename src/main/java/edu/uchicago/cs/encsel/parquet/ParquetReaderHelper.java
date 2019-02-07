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
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HiddenFileFilter;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ParquetReaderHelper {
    public static void read(URI file, ReaderProcessor processor) throws IOException,
            VersionParser.VersionParseException {
        Configuration conf = new Configuration();
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);
        List<FileStatus> statuses = Arrays.asList(fs.listStatus(path, HiddenFileFilter.INSTANCE));
        List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);
        if (footers.isEmpty()) {
            return;
        }

        ExecutorService threadPool = Executors.newFixedThreadPool(10);

        for (Footer footer : footers) {
            processor.processFooter(footer);
            VersionParser.ParsedVersion version = VersionParser.parse(footer.getParquetMetadata().getFileMetaData().getCreatedBy());

            ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile(), footer.getParquetMetadata());
            PageReadStore rowGroup = null;
            int blockCounter = 0;
            List<ColumnDescriptor> cols = footer.getParquetMetadata().getFileMetaData().getSchema().getColumns();
            while ((rowGroup = fileReader.readNextRowGroup()) != null) {
                final PageReadStore thisrowgroup = rowGroup;
                BlockMetaData blockMeta = footer.getParquetMetadata().getBlocks().get(blockCounter);
                threadPool.submit(() -> processor.processRowGroup(version, blockMeta, thisrowgroup));
                blockCounter++;
            }
        }
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static ProfileBean profile(URI file, ReaderProcessor processor) throws IOException,
            VersionParser.VersionParseException {
        Profiler p = new Profiler();
        p.mark();
        read(file,processor);
        return p.stop();
    }

}
