package edu.uchicago.cs.encsel.query.tpch;

import edu.uchicago.cs.encsel.model.FloatEncoding;
import edu.uchicago.cs.encsel.model.IntEncoding;
import edu.uchicago.cs.encsel.model.StringEncoding;
import edu.uchicago.cs.encsel.parquet.EncReaderProcessor;
import edu.uchicago.cs.encsel.parquet.ParquetReaderHelper;
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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.net.URI;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TPCHWorker {

    private CompressionCodecName[] codecs =
            {CompressionCodecName.UNCOMPRESSED, CompressionCodecName.LZO, CompressionCodecName.GZIP};

    private EncReaderProcessor processor;

    private MessageType schema;

    public TPCHWorker(EncReaderProcessor processor, MessageType schema) {
        this.processor = processor;
        this.schema = schema;
    }

    public void work(String filePath) throws Exception {
        File file = new File(filePath);

        for (int i = 0; i < schema.getColumns().size(); i++) {
            ColumnDescriptor cd = schema.getColumns().get(i);
            readColumn(file, i + 1, cd);
        }
    }

    protected void readColumn(File main, int index, ColumnDescriptor cd) throws Exception {
        List<String> encodings = null;
        switch (cd.getType()) {
            case BINARY:
                encodings = Arrays.stream(StringEncoding.values())
                        .filter(p -> p.parquetEncoding() != null).map(e -> e.name())
                        .collect(Collectors.toList());
                break;
            case INT32:
                encodings = Arrays.stream(IntEncoding.values())
                        .filter(p -> p.parquetEncoding() != null).map(e -> e.name())
                        .collect(Collectors.toList());
                break;
            case DOUBLE:
                encodings = Arrays.stream(FloatEncoding.values())
                        .filter(p -> p.parquetEncoding() != null).map(e -> e.name())
                        .collect(Collectors.toList());
                break;
            default:
                encodings = Collections.<String>emptyList();
                break;
        }

        for (String e : encodings) {
            for (CompressionCodecName codec : codecs) {
                String fileName = MessageFormat.format("{0}.col{1}.{2}_{3}",
                        main.getAbsolutePath(), index, e, codec.name());
                try {
                    ProfileBean loadTime = ParquetReaderHelper.profile(
                            new File(fileName).toURI(), processor);
                    System.out.println(MessageFormat.format("{0}, {1}, {2}, {3}",
                            index, e, codec.name(),
                            String.valueOf(loadTime.wallclock())));
                } catch (Exception ex) {
                    // Silently ignore
                }
            }
        }
    }
}
