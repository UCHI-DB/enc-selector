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

import java.io.File;
import java.io.IOException;

import static org.apache.parquet.filter2.predicate.FilterApi.*;


public class MOPRangeFilter {

    static int code = -2;
    static Binary date1993 = Binary.fromString("1992-01-02");
    static Binary date1994 = Binary.fromString("1992-01-03");
    static Binary min = Binary.fromString("1992-07-20");
    static Binary max = Binary.fromString("1993-03-28");
    static Boolean pred(int value){
        return value == 1;
    }
    static Boolean quantity_pred(int value) {return value<25; }
    static Boolean discount_pred(double value) {return (value>=0.05)&&(value<=0.07); }
    static Boolean shipdate_pred(Binary value) {return ( date1994.compareTo(value) > 0); }
    static Boolean hardShipdate_pred(int value) {return (value<code); }
    static int zoneMap(Binary value){
        if(value.compareTo(min)<0)
            return -1;
        else if (value.compareTo(max)>0)
            return 1;
        else
            return 0;
    }
    static int totalcount = 0;
    static int selected = 0;
    static Boolean outOfBoundary = false;
    static int zonemap;
    static int boundary = 0;
    static Boolean backup_pred(int value) {return (value>boundary); }

    public static void main(String[] args) throws IOException, VersionParser.VersionParseException {
        //args = new String[]{"false","1992-02-08", "false", "false"};
        if (args.length == 0) {
            System.out.println("ShipdataFilter order value pageskipping hardmode");
            return;
        }
        //code = Integer.parseInt(args[0]);
        String order = args[0];
        Boolean ordered = (order.equalsIgnoreCase("true") || order.equals("1"));
        date1994 = Binary.fromString(args[1]);
        String skip = args[2];
        String hard = args[3];
        boundary = 2273;
        Boolean pageSkipping = (skip.equalsIgnoreCase("true") || skip.equals("1"));
        Boolean hardmode = (hard.equalsIgnoreCase("true") || hard.equals("1"));

        System.out.println(date1994.toStringUsingUTF8()+":"+order+", skipmode:"+skip+", hardmode:"+hard);

        ColumnDescriptor l_quantity = TPCHSchema.lineitemSchema().getColumns().get(4);
        String quantity_str = Strings.join(l_quantity.getPath(), ".");
        ColumnDescriptor l_discount = TPCHSchema.lineitemSchema().getColumns().get(6);
        String discount_str = Strings.join(l_discount.getPath(), ".");
        ColumnDescriptor l_shipdate = TPCHSchema.lineitemSchema().getColumns().get(10);
        String shipdate_str = Strings.join(l_shipdate.getPath(), ".");
        ColumnDescriptor l_extendedprice = TPCHSchema.lineitemSchema().getColumns().get(5);
        FilterPredicate quantity_filter = lt(intColumn(quantity_str), 25);
        FilterPredicate discount_filter = and(gtEq(doubleColumn(discount_str), 0.05),ltEq(doubleColumn(discount_str), 0.07));
        FilterPredicate shipdate_filter = lt(binaryColumn(shipdate_str), date1994);
        FilterPredicate combine_filter = and(quantity_filter, and(shipdate_filter,discount_filter));
        FilterCompat.Filter rowGroup_filter = FilterCompat.get(shipdate_filter);
        String lineitem = "../tpch-generator/dbgen/lineitem";

        int intbound = 123;
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
            code = -2;
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
                        System.out.println("End detected!");
                        return;
                    }

                    if(hardmode){
                        if (!(EncContext.globalDict.get().containsKey(l_shipdate.toString()))||(code<0))
                        {
                            code = shipdateReader.retrieveDictID(date1994,ordered);
                            //System.out.println(code);
                            if (code > boundary){
                                code = 200;
                            }
                            zonemap = zoneMap(date1994);
                        }
                        int dictID = 0;
                        if (zonemap<0){
                            while(shipdateReader.getReadValue()<rowGroup.getRowCount()) {
                                long pageValueCount = shipdateReader.getPageValueCount();
                                long base = shipdateReader.getReadValue();
                                for (int j = 0; j<pageValueCount; j++){
                                    bitmap.set(base++, hardShipdate_pred(shipdateReader.getCurrentValueDictionaryID()));
                                    shipdateReader.consume();
                                }

                            }
                        }
                        else if(zonemap>0){
                            while(shipdateReader.getReadValue()<rowGroup.getRowCount()) {
                                long pageValueCount = shipdateReader.getPageValueCount();
                                long base = shipdateReader.getReadValue();
                                for (int j = 0; j<pageValueCount; j++){
                                    dictID = shipdateReader.getCurrentValueDictionaryID();
                                    bitmap.set(base++, backup_pred(dictID)||hardShipdate_pred(dictID));
                                    shipdateReader.consume();
                                }

                            }
                        }
                        else {
                            while(shipdateReader.getReadValue()<rowGroup.getRowCount()) {
                                long pageValueCount = shipdateReader.getPageValueCount();
                                long base = shipdateReader.getReadValue();
                                for (int j = 0; j<pageValueCount; j++){
                                    dictID = shipdateReader.getCurrentValueDictionaryID();
                                    if (dictID<=boundary){
                                        bitmap.set(base++, hardShipdate_pred(dictID));
                                    }
                                    else{
                                        bitmap.set(base++, shipdate_pred(shipdateReader.getBinary()));
                                    }
                                    shipdateReader.consume();
                                }

                            }
                        }
                    }
                    else{
                        while(shipdateReader.getReadValue()<rowGroup.getRowCount()) {
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

                    //int count = 0;
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

        if (true){
            code = -2;
            ProfileBean selectProf = ParquetReaderHelper.filterProfile(new File(lineitem+".parquet").toURI(), rowGroup_filter, new EncReaderProcessor() {

                @Override
                public void processRowGroup(VersionParser.ParsedVersion version,
                                            BlockMetaData meta, PageReadStore rowGroup) {
                    if(pageSkipping){
                        ParquetFileReader.setColFilter(rowGroup, l_shipdate, shipdate_filter);
                    }
                    RoaringBitmap bitmap = new RoaringBitmap();
                    //System.out.println("rowgroup count: "+rowGroup.getRowCount());
                    ColumnReaderImpl shipdateReader = new ColumnReaderImpl(l_shipdate, rowGroup.getPageReader(l_shipdate), new NonePrimitiveConverter(), version);

                    if(shipdateReader.getReadValue()>=rowGroup.getRowCount()) {
                        System.out.println("End detected!");
                        return;
                    }

                    if(hardmode){
                        if (!(EncContext.globalDict.get().containsKey(l_shipdate.toString()))||(code<0))
                        {
                            code = shipdateReader.retrieveDictID(date1994,ordered);
                            //System.out.println(code);
                            if (code > boundary){
                                code = 200;
                            }
                            zonemap = zoneMap(date1994);
                        }
                        int dictID = 0;
                        if (zonemap<0){
                            while(shipdateReader.getReadValue()<rowGroup.getRowCount()) {
                                long pageValueCount = shipdateReader.getPageValueCount();
                                long base = shipdateReader.getReadValue();
                                for (int j = 0; j<pageValueCount; j++){
                                    totalcount++;
                                    //System.out.println("dictID: "+shipdateReader.getCurrentValueDictionaryID());
                                    if (hardShipdate_pred(shipdateReader.getCurrentValueDictionaryID()))
                                        selected++;
                                    bitmap.set(base++, hardShipdate_pred(shipdateReader.getCurrentValueDictionaryID()));
                                    shipdateReader.consume();
                                }

                            }
                        }
                        else if(zonemap>0){
                            while(shipdateReader.getReadValue()<rowGroup.getRowCount()) {
                                long pageValueCount = shipdateReader.getPageValueCount();
                                long base = shipdateReader.getReadValue();
                                for (int j = 0; j<pageValueCount; j++){
                                    dictID = shipdateReader.getCurrentValueDictionaryID();
                                    totalcount++;
                                    if (backup_pred(dictID)||hardShipdate_pred(dictID))
                                        selected++;
                                    bitmap.set(base++, backup_pred(dictID)||hardShipdate_pred(dictID));
                                    shipdateReader.consume();
                                }

                            }
                        }
                        else {
                            while(shipdateReader.getReadValue()<rowGroup.getRowCount()) {
                                long pageValueCount = shipdateReader.getPageValueCount();
                                long base = shipdateReader.getReadValue();
                                for (int j = 0; j<pageValueCount; j++){
                                    totalcount++;
                                    dictID = shipdateReader.getCurrentValueDictionaryID();
                                    if (dictID<=boundary){
                                        bitmap.set(base++, hardShipdate_pred(dictID));
                                        if (hardShipdate_pred(dictID)){
                                            selected++;
                                        }
                                    }
                                    else{
                                        bitmap.set(base++, shipdate_pred(shipdateReader.getBinary()));
                                        if (shipdate_pred(shipdateReader.getBinary()))
                                            selected++;
                                    }
                                    shipdateReader.consume();
                                }

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
                                totalcount++;
                                if (shipdate_pred(shipdateReader.getBinary()))
                                    selected++;
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

                    //int count = 0;
                    //Object2IntMap shipdataDict = EncContext.globalDict.get().get(l_shipdate.toString());
                    //System.out.println("Dictioanry key value:"+ shipdataDict.toString());
                    //System.out.println("1993-01-01:" + shipdataDict.get(date1993) + " 1994-01-01: " + shipdataDict.get(date1994));

                }
            });
            System.out.println(String.format("%s,%d,%d,%d,%d,%d,%f", "last round", selectProf.wallclock(), selectProf.cpu(), selectProf.user(), selected, totalcount, 1.0*selected/totalcount));
        }
    }
}




