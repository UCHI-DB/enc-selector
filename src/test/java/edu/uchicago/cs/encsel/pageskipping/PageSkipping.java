package edu.uchicago.cs.encsel.pageskipping;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.junit.Test;

public class PageSkipping {

	@Test
	public void testPageSkipping() throws Exception{
		//Initialize new filter
		Filter filter = null;
		Configuration conf = new Configuration();
	    Path path =  new Path("/Users/chunwei/output.parquet");
	    ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
		MessageType schema = readFooter.getFileMetaData().getSchema();
		ParquetFileReader fileReader = new ParquetFileReader(conf, path, readFooter);
		fileReader.filterOutRow(filter);
		PageReadStore PageReaders = fileReader.readNextRowGroup();
		
		
		

	}

	@Test
	public void testPageSkipp() {
		fail("Not yet implemented");
	}
}
