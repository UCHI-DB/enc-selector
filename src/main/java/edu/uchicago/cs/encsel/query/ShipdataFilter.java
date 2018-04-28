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
import edu.uchicago.cs.encsel.query.bitmap.RoaringBitmap;
import edu.uchicago.cs.encsel.query.filter.StatisticsPageFilter;
import edu.uchicago.cs.encsel.query.tpch.TPCHSchema;
import edu.uchicago.cs.encsel.util.perf.ProfileBean;
import org.apache.parquet.Strings;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntMap;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.apache.parquet.filter2.predicate.FilterApi.*;


public class ShipdataFilter {

    static int code = 0;
    static Binary date1993 = Binary.fromString("1992-01-02");
    static Binary date1994 = Binary.fromString("1992-01-03");
    static Boolean pred(int value){
        return value == 1;
    }
    static Boolean quantity_pred(int value) {return value<25; }
    static Boolean discount_pred(double value) {return (value>=0.05)&&(value<=0.07); }
    static Boolean shipdate_pred(Binary value) {return (date1993.compareTo(value)==0)||(date1993.compareTo(value) + date1994.compareTo(value) == 0); }
    static Boolean hardShipdate_pred(int value) {return  (value>=0)&&(value<code); }

    public static void main(String[] args) throws IOException, VersionParser.VersionParseException {
        args = new String[]{"1","1992-01-03", "false", "false"};
        if (args.length == 0) {
            System.out.println("ShipdataFilter code value pageskipping hardmode");
            return;
        }
        code = Integer.parseInt(args[0]);
        date1994 = Binary.fromString(args[1]);
        String skip = args[2];
        String hard = args[3];
        Boolean pageSkipping = (skip.equalsIgnoreCase("true") || skip.equals("1"));
        Boolean hardmode = (hard.equalsIgnoreCase("true") || hard.equals("1"));

        System.out.println(date1994.toStringUsingUTF8()+":"+code+", skipmode:"+skip+", hardmode:"+hard);

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
                    if(pageSkipping){
                        ParquetFileReader.setColFilter(rowGroup, l_shipdate, shipdate_filter);
                    }
                    RoaringBitmap bitmap = new RoaringBitmap();
                    //System.out.println("rowgroup count: "+rowGroup.getRowCount());
                    ColumnReaderImpl shipdateReader = new ColumnReaderImpl(l_shipdate, rowGroup.getPageReader(l_shipdate), new NonePrimitiveConverter(), version);
                    /*for (long j = 0;  j<rowGroup.getRowCount(); j++) {
                        bitmap.set(j, shipdate_pred(shipdateReader.getBinary()));
                        //bitmap.set(j, hardShipdate_pred(shipdateReader.getDictId()));
                        //if (quantity_pred(value))
                        //System.out.println("row number:" + j + " value: " + colReader.getInteger());
                        shipdateReader.consume();
                    }*/


                    if(shipdateReader.getReadValue()>=rowGroup.getRowCount()) {
                        //System.out.println("End detected!");
                        return;
                    }

                    if(hardmode){
                        while(shipdateReader.getReadValue()<rowGroup.getRowCount()) {
                            //System.out.println("getReadValue:"+shipdateReader.getReadValue());
                            //System.out.println("getPageValueCount:"+shipdateReader.getPageValueCount());
                            long pageValueCount = shipdateReader.getPageValueCount();
                            long base = shipdateReader.getReadValue();
                            for (int j = 0; j<pageValueCount; j++){
                                //System.out.println("row number:" + shipdateReader.getReadValue());
                                //bitmap.set(base++, shipdate_pred(shipdateReader.getBinary()));
                                bitmap.set(base++, hardShipdate_pred(shipdateReader.getDictId()));
                                shipdateReader.consume();
                            }

                        }
                    }
                    else{
                        while(shipdateReader.getReadValue()<rowGroup.getRowCount()) {
                            //System.out.println("getReadValue:"+shipdateReader.getReadValue());
                            //System.out.println("getPageValueCount:"+shipdateReader.getPageValueCount());
                            long pageValueCount = shipdateReader.getPageValueCount();
                            long base = shipdateReader.getReadValue();
                            for (int j = 0; j<pageValueCount; j++){
                                //System.out.println("row number:" + shipdateReader.getReadValue());
                                bitmap.set(base++, shipdate_pred(shipdateReader.getBinary()));
                                //bitmap.set(base++, hardShipdate_pred(shipdateReader.getDictId()));
                                shipdateReader.consume();
                            }

                        }
                    }
                    if(shipdateReader.getReadValue()>=rowGroup.getRowCount()) {
                        //System.out.println("End detected!");
                        return;
                    }

                    int count = 0;
                    //Object2IntMap shipdataDict = EncContext.globalDict.get().get(l_shipdate.toString());
                    //System.out.println("Dictioanry key value:"+ shipdataDict.toString());
                    //System.out.println("1993-01-01:" + shipdataDict.get(date1993) + " 1994-01-01: " + shipdataDict.get(date1994));

                }
            });

            System.out.println(String.format("%s,%d,%d,%d", "round"+i, prof.wallclock(), prof.cpu(), prof.user()));
            clocktime = clocktime + prof.wallclock();
            cputime = cputime + prof.cpu();
            usertime = usertime + prof.user();
        }
        System.out.println(String.format("%s,%d,%d,%d,%d,%d", "ScanOnheap", clocktime / repeat, cputime / repeat, usertime / repeat, StatisticsPageFilter.getPAGECOUNT() / repeat,StatisticsPageFilter.getPAGESKIPPED() / repeat));

    }
}





