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
 * under the License,
 *
 * Contributors:
 *     Chunwei Liu - initial API and implementation
 *
 */

package edu.uchicago.cs.encsel.query;

import edu.uchicago.cs.encsel.parquet.EncContext;
import edu.uchicago.cs.encsel.parquet.EncReaderProcessor;
import edu.uchicago.cs.encsel.parquet.ParquetReaderHelper;
import edu.uchicago.cs.encsel.parquet.ParquetWriterHelper;
import edu.uchicago.cs.encsel.util.perf.ProfileBean;
import edu.uchicago.cs.encsel.util.perf.Profiler;
import edu.uchicago.cs.encsel.query.bitmap.RoaringBitmap;
import edu.uchicago.cs.encsel.query.operator.HashJoin;
import edu.uchicago.cs.encsel.query.tpch.TPCHSchema;
import org.apache.parquet.Strings;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntMap;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;



public class Q6ScanTool {

    static Binary date1993 = Binary.fromString("1993-01-01");
    static Binary date1994 = Binary.fromString("1994-01-01");
    static Boolean pred(int value){
        return value == 1;
    }
    static Boolean quantity_pred(int value) {return value<25; }
    static Boolean discount_pred(double value) {return (value>=0.05)&&(value<=0.07); }
    static Boolean shipdate_pred(Binary value) {return (date1993.compareTo(value)==0)||(date1993.compareTo(value) + date1994.compareTo(value) == 0); }
    static Boolean hardShipdate_pred(int value) {return  (value>=365)&&(value<730); }

    public static void main(String[] args) throws IOException, VersionParser.VersionParseException {

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
        FilterCompat.Filter rowGroup_filter = FilterCompat.get(combine_filter);
        String lineitem = "../tpch-generator/dbgen/lineitem";

        int intbound = ParquetWriterHelper.scanIntMaxInTab(new File(lineitem+".tbl").toURI(), 4);
        int bitLength = 32 - Integer.numberOfLeadingZeros(intbound);
        System.out.println("lineitem intBitLength: "+ bitLength +" lineitem intBound: "+intbound);
        EncContext.context.get().put(TPCHSchema.lineitemSchema().getColumns().get(4).toString(), new Integer[]{bitLength,intbound});

		/*EncReaderProcessor p = new EncReaderProcessor() {
			@Override
			public void processRowGroup(VersionParser.ParsedVersion version,
										BlockMetaData meta, PageReadStore rowGroup) {

			}
		};*/

        int repeat = 10;
        long clocktime = 0L;
        long cputime = 0L;
        long usertime = 0L;

        for (int i = 0; i < repeat; i++) {
            ProfileBean prof = ParquetReaderHelper.filterProfile(new File(lineitem+".parquet").toURI(), rowGroup_filter, new EncReaderProcessor() {

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
                    Object2IntMap shipdataDict = EncContext.globalDict.get().get(l_shipdate.toString());
                    //System.out.println("1993-01-01:" + shipdataDict.get(date1993) + " 1994-01-01: " + shipdataDict.get(date1994));
                    ColumnReaderImpl shipdateReader = new ColumnReaderImpl(l_shipdate, rowGroup.getPageReader(l_shipdate), new NonePrimitiveConverter(), version);
                    for (long j = 0;  j<rowGroup.getRowCount(); j++) {
                        if (bitmap.test(j)) {
                            count++;
                            //System.out.println(shipdateReader.getBinary().toStringUsingUTF8());
                            //bitmap.set(j, shipdate_pred(shipdateReader.getBinary()));
                            bitmap.set(j, hardShipdate_pred(shipdateReader.getCurrentValueDictionaryID()));
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

            System.out.println(String.format("%s,%d,%d,%d", "round"+i, prof.wallclock(), prof.cpu(), prof.user()));
            clocktime = clocktime + prof.wallclock();
            cputime = cputime + prof.cpu();
            usertime = usertime + prof.user();
        }
        System.out.println(String.format("%s,%d,%d,%d", "ScanOnheap", clocktime / repeat, cputime / repeat, usertime / repeat));

    }
}





