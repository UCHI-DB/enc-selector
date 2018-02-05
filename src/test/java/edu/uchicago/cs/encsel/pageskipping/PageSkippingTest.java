package edu.uchicago.cs.encsel.pageskipping;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.Version;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;

import static junit.framework.Assert.assertEquals;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import org.junit.Test;

import edu.uchicago.cs.encsel.query.bitmap.Bitmap;
import edu.uchicago.cs.encsel.query.bitmap.TrivialBitmap;

public class PageSkippingTest {
	private static final class ValidatingConverter extends PrimitiveConverter {
	    int count = 1;

	    @Override
	    public void addInt(int value) {
	      //assertEquals("bar" + count % 10, value.toStringUsingUTF8());
	    	assertEquals(count,value);
	      ++ count;
	    }
	}
	
	@Test
	public void testOffheap() throws Exception{
		//TODO Initialize new filter
		IntColumn cust_key = intColumn("cust_key");
		FilterPredicate pred = eq(cust_key, 1000000);
		Filter filter = FilterCompat.get(eq(cust_key, 100000000));
		ColumnDescriptor descriptor = null;
		Bitmap bitmap = new TrivialBitmap(100);
		bitmap.set(10, true);
		assertTrue(bitmap.test(10));
		assertFalse(bitmap.test(1));
		long startpos = 0;
		Configuration conf = new Configuration();
	    Path path =  new Path("src/test/resource/query_select/customer_100.parquet");
	    ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
		MessageType schema = readFooter.getFileMetaData().getSchema();
		descriptor = schema.getColumnDescription(schema.getPaths().get(0));
		
		//System.out.print( schema.getPaths().get(0)[0]);
		ParquetFileReader fileReader = new ParquetFileReader(conf, path, readFooter);
		assertEquals( fileReader.getRowGroups().size(), 1);
		//fileReader.filterOutRow(filter);
		//System.out.println( fileReader.getRowGroups().size());
		//System.out.println( descriptor.toString());

		PageReadStore pageReaders = null;
		int i = 0;
		pageReaders=fileReader.readNextRowGroup();
		while(pageReaders != null) {
			//Use static method to set predicate filter
			//ParquetFileReader.setColFilter(pageReaders, descriptor, pred);
			//Use static method to set predicate filter
			ParquetFileReader.setColBitmap(pageReaders, descriptor, bitmap, 0);
			
			PageReader columnChunkPageReader = pageReaders.getPageReader(descriptor);
			PrimitiveConverter converter = new ValidatingConverter();
			//System.out.println("rowgroupID: "+i);
			ColumnReaderImpl columnReader = new ColumnReaderImpl(descriptor, columnChunkPageReader, converter, VersionParser.parse(Version.FULL_VERSION));
			columnReader.getoffheapSupport();
			
			//columnReader.consume();
			assertEquals(columnReader.getReadValue(),100);
			assertEquals(columnReader.getPageValueCount(),100);
			if(columnReader.getReadValue()>=pageReaders.getRowCount()) {
				//System.out.println("End detected!");
				pageReaders=fileReader.readNextRowGroup();
				continue;
			}
			columnReader.writeCurrentValueToConverter();
			//System.out.println(columnReader.getPageInf().getStartPos());
			//System.out.println(columnReader.getPageInf().getValueCount());
			columnReader.consume();
			columnReader.writeCurrentValueToConverter();
			pageReaders=fileReader.readNextRowGroup();
			i++;
		}
	}
	
	@Test
	public void testBitmapPageSkipping() throws Exception{
		//TODO Initialize new filter
		IntColumn cust_key = intColumn("cust_key");
		FilterPredicate pred = eq(cust_key, 1000000);
		Filter filter = FilterCompat.get(eq(cust_key, 100000000));
		ColumnDescriptor descriptor = null;
		Bitmap bitmap = new TrivialBitmap(100);
		//bitmap.set(10, true);
		//System.out.println( bitmap.test(1));
		long startpos = 0;
		Configuration conf = new Configuration();
	    Path path =  new Path("src/test/resource/query_select/customer_100.parquet");
	    ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
		MessageType schema = readFooter.getFileMetaData().getSchema();
		descriptor = schema.getColumnDescription(new String[] {"cust_key"});
		
		//System.out.print( schema.toString());
		ParquetFileReader fileReader = new ParquetFileReader(conf, path, readFooter);
		//System.out.println( fileReader.getRowGroups().size());
		//fileReader.filterOutRow(filter);
		//System.out.println( fileReader.getRowGroups().size());
		//System.out.println( descriptor.toString());

		PageReadStore pageReaders = null;
		int i = 0;
		pageReaders=fileReader.readNextRowGroup();
		while(pageReaders != null) {
			//Use static method to set predicate filter
			//ParquetFileReader.setColFilter(pageReaders, descriptor, pred);
			//Use static method to set predicate filter
			ParquetFileReader.setColBitmap(pageReaders, descriptor, bitmap, 0);
			
			PageReader columnChunkPageReader = pageReaders.getPageReader(descriptor);
			PrimitiveConverter converter = new ValidatingConverter();
			//System.out.println("rowgroupID: "+i);
			ColumnReaderImpl columnReader = new ColumnReaderImpl(descriptor, columnChunkPageReader, converter, VersionParser.parse(Version.FULL_VERSION));
			assertEquals(columnReader.getReadValue(), 100);
			if(columnReader.getReadValue()>=pageReaders.getRowCount()) {
				//System.out.println("End detected!");
				pageReaders=fileReader.readNextRowGroup();
				continue;
			}
			columnReader.getoffheapSupport();
			
			//columnReader.consume();
			//System.out.println("getReadValue:"+columnReader.getReadValue());
			//System.out.println("getPageValueCount:"+columnReader.getPageValueCount());
			
			columnReader.writeCurrentValueToConverter();
			//System.out.println(columnReader.getPageInf().getStartPos());
			//System.out.println(columnReader.getPageInf().getValueCount());
			columnReader.consume();
			columnReader.writeCurrentValueToConverter();
			pageReaders=fileReader.readNextRowGroup();
			i++;
		}
	}
	

	@Test
	public void testPredPageSkipping() throws Exception{
		//TODO Initialize new filter
		IntColumn cust_key = intColumn("cust_key");
		FilterPredicate pred = eq(cust_key, 1000000);
		Filter filter = FilterCompat.get(eq(cust_key, 100000000));
		ColumnDescriptor descriptor = null;
		Bitmap bitmap = new TrivialBitmap(100);
		bitmap.set(10, true);
		//System.out.println( bitmap.test(1));
		long startpos = 0;
		Configuration conf = new Configuration();
	    Path path =  new Path("src/test/resource/query_select/customer_100.parquet");
	    ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
		MessageType schema = readFooter.getFileMetaData().getSchema();
		descriptor = schema.getColumnDescription(new String[] {"cust_key"});
		
		//System.out.print( schema.toString());
		ParquetFileReader fileReader = new ParquetFileReader(conf, path, readFooter);
		//System.out.println( fileReader.getRowGroups().size());
		//fileReader.filterOutRow(filter);
		//System.out.println( fileReader.getRowGroups().size());
		//System.out.println( descriptor.toString());

		PageReadStore pageReaders = null;
		int i = 0;
		pageReaders=fileReader.readNextRowGroup();
		while(pageReaders != null) {
			//Use static method to set predicate filter
			//ParquetFileReader.setColFilter(pageReaders, descriptor, pred);
			//Use static method to set predicate filter
			ParquetFileReader.setColBitmap(pageReaders, descriptor, bitmap, 0);
			
			PageReader columnChunkPageReader = pageReaders.getPageReader(descriptor);
			PrimitiveConverter converter = new ValidatingConverter();
			//System.out.println("rowgroupID: "+i);
			ColumnReaderImpl columnReader = new ColumnReaderImpl(descriptor, columnChunkPageReader, converter, VersionParser.parse(Version.FULL_VERSION));
			
			//columnReader.consume();
			//System.out.println("getReadValue:"+columnReader.getReadValue());
			assertEquals(columnReader.getPageValueCount(),100);
			if(columnReader.getReadValue()>=pageReaders.getRowCount()) {
				//System.out.println("End detected!");
				pageReaders=fileReader.readNextRowGroup();
				continue;
			}
			columnReader.writeCurrentValueToConverter();
			assertEquals(columnReader.getPageInf().getStartPos(),0);
			//System.out.println(columnReader.getPageInf().getValueCount());
			columnReader.consume();
			columnReader.writeCurrentValueToConverter();
			pageReaders=fileReader.readNextRowGroup();
			i++;
		}
	}

}
