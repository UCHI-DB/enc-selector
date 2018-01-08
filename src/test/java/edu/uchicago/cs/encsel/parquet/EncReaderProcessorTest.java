package edu.uchicago.cs.encsel.parquet;

import org.apache.parquet.VersionParser;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class EncReaderProcessorTest {

    @Test
    public void testRead() throws Exception {

        EncReaderProcessor p = new EncReaderProcessor() {
            @Override
            public void processRowGroup(VersionParser.ParsedVersion version,
                                        BlockMetaData meta, PageReadStore rowGroup) {

            }
        };

        Map<String, Object[]> context = EncContext.context.get();
        context.clear();
        assertEquals(0, context.size());
        ParquetReaderHelper.read(new File("src/test/resource/coldata/test_bp_int").toURI(), p);


        assertEquals(1, context.size());
        assertEquals("26", context.get("[value] INT32")[0]);
        assertEquals("67108863", context.get("[value] INT32")[1]);
    }
}
