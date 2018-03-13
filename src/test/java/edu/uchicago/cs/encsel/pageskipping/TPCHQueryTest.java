package edu.uchicago.cs.encsel.pageskipping;

import edu.uchicago.cs.encsel.parquet.OffheapReadSopport;
import edu.uchicago.cs.encsel.util.perf.ProfileBean;
import edu.uchicago.cs.encsel.util.perf.Profiler;
import edu.uchicago.cs.encsel.query.NonePrimitiveConverter;
import edu.uchicago.cs.encsel.query.bitmap.Bitmap;
import edu.uchicago.cs.encsel.query.bitmap.RoaringBitmap;
import edu.uchicago.cs.encsel.query.bitmap.TrivialBitmap;
import edu.uchicago.cs.encsel.query.offheap.EqualScalar;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Strings;
import org.apache.parquet.Version;
import edu.uchicago.cs.encsel.parquet.EncReaderProcessor;
import edu.uchicago.cs.encsel.parquet.ParquetReaderHelper;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;
import edu.uchicago.cs.encsel.query.tpch.TPCHSchema;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;

import static junit.framework.Assert.assertEquals;
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TPCHQueryTest {

	private static final class ValidatingConverter extends PrimitiveConverter {
		long count = 1;
		public void setCount(long count) {
			this.count = count;
		}
		@Override
		public void addInt(int value) {
			//assertEquals("bar" + count % 10, value.toStringUsingUTF8());
			if(value == 1 || value == 65486)
				//System.out.println(value +" row number:" + count);
			//assertEquals(count,value);
			++ count;
		}
	}

	private Binary date1993 = Binary.fromString("1993-01-01");
	private Binary date1994 = Binary.fromString("1994-01-01");
	private Boolean pred(int value){
		return value == 1;
	}
	private Boolean quantity_pred(int value) {return value<25; }
	private Boolean discount_pred(double value) {return (value>=0.05)&&(value<=0.07); }
	private Boolean shipdate_pred(Binary value) {return (date1993.compareTo(value)==0)||(date1993.compareTo(value) + date1994.compareTo(value) == 0); }


	@Test
	public void testOriginOnheap() throws Exception{

		ColumnDescriptor cd = TPCHSchema.orderSchema().getColumns().get(1);

		/*EncReaderProcessor p = new EncReaderProcessor() {
			@Override
			public void processRowGroup(VersionParser.ParsedVersion version,
										BlockMetaData meta, PageReadStore rowGroup) {

			}
		};*/

		int repeat = 20;
		long clocktime = 0L;
		long cputime = 0L;
		long usertime = 0L;
		for (int i = 0; i < repeat; i++) {
			ProfileBean prof = ParquetReaderHelper.profile(new File("../tpch-generator/dbgen/orders.parquet").toURI(), new EncReaderProcessor() {

				@Override
				public void processRowGroup(VersionParser.ParsedVersion version,
											BlockMetaData meta, PageReadStore rowGroup) {
					//ParquetFileReader.setColFilter(pageReaders, cd, pred);
					ColumnReaderImpl colReader = new ColumnReaderImpl(cd, rowGroup.getPageReader(cd), new NonePrimitiveConverter(), version);
					RoaringBitmap bitmap = new RoaringBitmap();
					for (long j = 0;  j<rowGroup.getRowCount(); j++) {
						bitmap.set(j, pred(colReader.getInteger()));
						if (pred(colReader.getInteger()))
							//System.out.println("row number:" + j + " value: " + colReader.getInteger());
						colReader.consume();

					}

				}
			});

			clocktime = clocktime + prof.wallclock();
			cputime = cputime + prof.cpu();
			usertime = usertime + prof.user();
		}
		System.out.println(String.format("%s,%d,%d,%d", "Onfffheap", clocktime / repeat, cputime / repeat, usertime / repeat));
	}

	@Test
	public void testQ6Onheap() throws Exception{
		ColumnDescriptor l_quantity = TPCHSchema.lineitemSchema().getColumns().get(4);
		String quantity_str = Strings.join(l_quantity.getPath(), ".");
		ColumnDescriptor l_discount = TPCHSchema.lineitemSchema().getColumns().get(6);
		String discount_str = Strings.join(l_discount.getPath(), ".");
		ColumnDescriptor l_shipdate = TPCHSchema.lineitemSchema().getColumns().get(10);
		String shipdate_str = Strings.join(l_shipdate.getPath(), ".");
		ColumnDescriptor l_extendedprice = TPCHSchema.lineitemSchema().getColumns().get(5);
		FilterPredicate quantity_filter = lt(intColumn(quantity_str), 25);
		FilterPredicate discount_filter = and(gtEq(doubleColumn(discount_str), 0.05),ltEq(doubleColumn(discount_str), 0.07));
		FilterPredicate shipdate_filter = and(gtEq(binaryColumn(shipdate_str), date1993), lt(binaryColumn(shipdate_str), date1994));
		FilterPredicate combine_filter = and(quantity_filter, and(shipdate_filter,discount_filter));
		Filter rowGroup_filter = FilterCompat.get(combine_filter);

		/*EncReaderProcessor p = new EncReaderProcessor() {
			@Override
			public void processRowGroup(VersionParser.ParsedVersion version,
										BlockMetaData meta, PageReadStore rowGroup) {

			}
		};*/

		int repeat = 5;
		long clocktime = 0L;
		long cputime = 0L;
		long usertime = 0L;

		for (int i = 0; i < repeat; i++) {
			ProfileBean prof = ParquetReaderHelper.filterProfile(new File("../tpch-generator/dbgen/lineitem.parquet").toURI(), rowGroup_filter, new EncReaderProcessor() {

				@Override
				public void processRowGroup(VersionParser.ParsedVersion version,
											BlockMetaData meta, PageReadStore rowGroup) {
					//ParquetFileReader.setColFilter(pageReaders, cd, pred);
					RoaringBitmap bitmap = new RoaringBitmap();
					//System.out.println("rowgroup count: "+rowGroup.getRowCount());
					ColumnReaderImpl quantityReader = new ColumnReaderImpl(l_quantity, rowGroup.getPageReader(l_quantity), new NonePrimitiveConverter(), version);
					for (long j = 0;  j<rowGroup.getRowCount(); j++) {
						bitmap.set(j, quantity_pred(quantityReader.getInteger()));
						//if (quantity_pred(value))
						//System.out.println("row number:" + j + " value: " + colReader.getInteger());
						quantityReader.consume();
					}

					int count = 0;
					ColumnReaderImpl shipdateReader = new ColumnReaderImpl(l_shipdate, rowGroup.getPageReader(l_shipdate), new NonePrimitiveConverter(), version);
					for (long j = 0;  j<rowGroup.getRowCount(); j++) {
						if (bitmap.test(j)) {
							count++;
							bitmap.set(j, shipdate_pred(shipdateReader.getBinary()));
							//System.out.println("test  ----- row number:" + j );
						}
						else
							shipdateReader.skip();
						shipdateReader.consume();
					}

					//System.out.println("after quantity: "+count);
					ArrayList<Double> discountVals = new ArrayList<Double>();

					ColumnReaderImpl discountReader = new ColumnReaderImpl(l_discount, rowGroup.getPageReader(l_discount), new NonePrimitiveConverter(), version);
					count = 0;
					double cur = 0;
					for (long j = 0;  j<rowGroup.getRowCount(); j++) {
						if (bitmap.test(j)) {
							count++;
							cur = discountReader.getDouble();
							if (discount_pred(cur)) {
								discountVals.add(cur);
							}
							else
								bitmap.set(j, discount_pred(cur));
							//System.out.println("test  ----- row number:" + j + " value: " + colReader1.getBinary().toStringUsingUTF8());
						}
						else
							discountReader.skip();
						discountReader.consume();
					}

					//System.out.println("after shipdate: "+count+"list:"+ discountVals.size());

					count = 0;

					ColumnReaderImpl extendedpriceReader = new ColumnReaderImpl(l_extendedprice, rowGroup.getPageReader(l_extendedprice), new NonePrimitiveConverter(), version);
					count = 0;
					double revenue = 0;
					for (long j = 0;  j<rowGroup.getRowCount(); j++) {
						if (bitmap.test(j)) {
							count++;
							cur = extendedpriceReader.getDouble();
							revenue += cur*discountVals.remove(0);
							//System.out.println("test  ----- row number:" + j + " value: " + colReader1.getBinary().toStringUsingUTF8());
						}
						else
							extendedpriceReader.skip();
						extendedpriceReader.consume();
					}

					//System.out.println("after discount: "+count+"list:"+ discountVals.size());

					//System.out.println("$$revenue: "+revenue);
				}
			});

			clocktime = clocktime + prof.wallclock();
			cputime = cputime + prof.cpu();
			usertime = usertime + prof.user();
			System.out.println(String.format("%s,%d,%d,%d", "Onheap"+i, prof.wallclock(), prof.cpu(), prof.user()));
		}
		System.out.println(String.format("%s,%d,%d,%d", "Onheap", clocktime / repeat, cputime / repeat, usertime / repeat));
	}

	@Test
	public void testQ6SkipOnheap() throws Exception{
		ColumnDescriptor l_quantity = TPCHSchema.lineitemSchema().getColumns().get(4);
		String quantity_str = Strings.join(l_quantity.getPath(), ".");
		ColumnDescriptor l_discount = TPCHSchema.lineitemSchema().getColumns().get(6);
		String discount_str = Strings.join(l_discount.getPath(), ".");
		ColumnDescriptor l_shipdate = TPCHSchema.lineitemSchema().getColumns().get(10);
		String shipdate_str = Strings.join(l_shipdate.getPath(), ".");
		ColumnDescriptor l_extendedprice = TPCHSchema.lineitemSchema().getColumns().get(5);
		FilterPredicate quantity_filter = lt(intColumn(quantity_str), 25);
		FilterPredicate discount_filter = and(gtEq(doubleColumn(discount_str), 0.05),ltEq(doubleColumn(discount_str), 0.07));
		FilterPredicate shipdate_filter = and(gtEq(binaryColumn(shipdate_str), date1993), lt(binaryColumn(shipdate_str), date1994));
		FilterPredicate combine_filter = and(quantity_filter, and(shipdate_filter,discount_filter));
		Filter rowGroup_filter = FilterCompat.get(combine_filter);

		/*EncReaderProcessor p = new EncReaderProcessor() {
			@Override
			public void processRowGroup(VersionParser.ParsedVersion version,
										BlockMetaData meta, PageReadStore rowGroup) {

			}
		};*/

		int repeat = 5;
		long clocktime = 0L;
		long cputime = 0L;
		long usertime = 0L;

		for (int i = 0; i < repeat; i++) {
			ProfileBean prof = ParquetReaderHelper.filterProfile(new File("../tpch-generator/dbgen/lineitem.parquet").toURI(), rowGroup_filter, new EncReaderProcessor() {

				@Override
				public void processRowGroup(VersionParser.ParsedVersion version,
											BlockMetaData meta, PageReadStore rowGroup) {

					//First predicate
					//set filter
					ParquetFileReader.setColFilter(rowGroup, l_quantity, quantity_filter);
					ParquetFileReader.setColFilter(rowGroup, l_discount, discount_filter);
					ParquetFileReader.setColFilter(rowGroup, l_shipdate, shipdate_filter);
					//set bitmap
					//ParquetFileReader.setColBitmap(rowGroup, l_quantity, bitmap, 0);

					RoaringBitmap bitmap = new RoaringBitmap();
					//System.out.println("rowgroup count: "+rowGroup.getRowCount());
					ColumnReaderImpl quantityReader = new ColumnReaderImpl(l_quantity, rowGroup.getPageReader(l_quantity), new NonePrimitiveConverter(), version);

					while(quantityReader.getReadValue()<rowGroup.getRowCount()) {
						//System.out.println("getReadValue:" + quantityReader.getReadValue());
						//System.out.println("getPageValueCount:" + quantityReader.getPageValueCount());
						long pageValueCount = quantityReader.getPageValueCount();
						long offset = quantityReader.getReadValue();
						for (int j = 0; j < pageValueCount; j++) {
							//System.out.println("row number:" + columnReader.getReadValue());
							bitmap.set(offset + j, quantity_pred(quantityReader.getInteger()));
							quantityReader.consume();
						}

					}


					//second predicate
					int count = 0;
					ParquetFileReader.setColBitmap(rowGroup, l_shipdate, bitmap, 0);
					ColumnReaderImpl shipdateReader = new ColumnReaderImpl(l_shipdate, rowGroup.getPageReader(l_shipdate), new NonePrimitiveConverter(), version);

					while(shipdateReader.getReadValue()<rowGroup.getRowCount()) {
						//System.out.println("getReadValue:" + shipdateReader.getReadValue());
						//System.out.println("getPageValueCount:" + shipdateReader.getPageValueCount());
						long pageValueCount = shipdateReader.getPageValueCount();
						long offset = shipdateReader.getReadValue();
						for (int j = 0; j < pageValueCount; j++) {
							//System.out.println("row number:" + columnReader.getReadValue());
							long pos = offset + j;
							if (bitmap.test(pos)){
								count++;
								bitmap.set(pos, shipdate_pred(shipdateReader.getBinary()));
							}
							else
								shipdateReader.skip();
							shipdateReader.consume();
						}

					}
					//System.out.println("after quantity: "+count +" Pageskipped "+ shipdateReader.getPageskipped());

					// third predicate
					ArrayList<Double> discountVals = new ArrayList<Double>();
					ParquetFileReader.setColBitmap(rowGroup, l_discount, bitmap, 0);
					ColumnReaderImpl discountReader = new ColumnReaderImpl(l_discount, rowGroup.getPageReader(l_discount), new NonePrimitiveConverter(), version);
					count = 0;
					double cur = 0;

					while(discountReader.getReadValue()<rowGroup.getRowCount()) {
						//System.out.println("getReadValue:" + discountReader.getReadValue());
						//System.out.println("getPageValueCount:" + discountReader.getPageValueCount());
						long pageValueCount = discountReader.getPageValueCount();
						long offset = discountReader.getReadValue();
						for (int j = 0; j < pageValueCount; j++) {
							//System.out.println("row number:" + columnReader.getReadValue());
							long pos = offset + j;
							if (bitmap.test(pos)) {
								count++;
								cur = discountReader.getDouble();
								if (discount_pred(cur)) {
									discountVals.add(cur);
								}
								else
									bitmap.set(pos, discount_pred(cur));
								//System.out.println("test  ----- row number:" + j + " value: " + colReader1.getBinary().toStringUsingUTF8());
							}
							else
								discountReader.skip();
							discountReader.consume();

						}

					}

					//System.out.println("after shipdate: "+count+"Pageskipped"+ discountReader.getPageskipped() +" list:"+ discountVals.size());

					// compute sum
					count = 0;
					ParquetFileReader.setColBitmap(rowGroup, l_extendedprice, bitmap, 0);
					ColumnReaderImpl extendedpriceReader = new ColumnReaderImpl(l_extendedprice, rowGroup.getPageReader(l_extendedprice), new NonePrimitiveConverter(), version);
					count = 0;
					double revenue = 0;
					while(extendedpriceReader.getReadValue()<rowGroup.getRowCount()) {
						//System.out.println("getReadValue:" + quantityReader.getReadValue());
						//System.out.println("getPageValueCount:" + quantityReader.getPageValueCount());
						long pageValueCount = extendedpriceReader.getPageValueCount();
						long offset = extendedpriceReader.getReadValue();
						for (int j = 0; j < pageValueCount; j++) {
							//System.out.println("row number:" + columnReader.getReadValue());
							long pos = offset + j;
							if (bitmap.test(pos)) {
								count++;
								cur = extendedpriceReader.getDouble();
								revenue += cur*discountVals.remove(0);
								//System.out.println("test  ----- row number:" + j + " value: " + colReader1.getBinary().toStringUsingUTF8());
							}
							else
								extendedpriceReader.skip();
							extendedpriceReader.consume();
						}

					}

					//System.out.println("after discount: "+count+"Pageskipped"+ extendedpriceReader.getPageskipped() +" list: "+ discountVals.size());

					//System.out.println("$$revenue: "+revenue);
				}
			});

			clocktime = clocktime + prof.wallclock();
			cputime = cputime + prof.cpu();
			usertime = usertime + prof.user();
			System.out.println(String.format("%s,%d,%d,%d", "Onheap"+i, prof.wallclock(), prof.cpu(), prof.user()));
		}
		System.out.println(String.format("%s,%d,%d,%d", "Skip-Onheap", clocktime / repeat, cputime / repeat, usertime / repeat));
	}

	@Test
	public void binaryTest(){
		Binary byte1 = Binary.fromString("1993-01-01");
		Binary byte2 = Binary.fromString("1994-01-01");
		System.out.println("byte1.compareTo(byte2): "+byte1.compareTo(byte2)+" byte2.compareTo(byte1): "+byte2.compareTo(byte1));
		ColumnDescriptor l_quantity = TPCHSchema.lineitemSchema().getColumns().get(4);
		System.out.println(Strings.join(l_quantity.getPath(), "."));
	}
	
	
}
