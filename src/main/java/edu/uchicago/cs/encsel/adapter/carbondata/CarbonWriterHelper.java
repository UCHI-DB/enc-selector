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
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */

package edu.uchicago.cs.encsel.adapter.carbondata;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.commons.io.FileUtils;
//
//import org.apache.carbondata.sdk.file.CarbonWriter;
//import org.apache.carbondata.sdk.file.Field;
//import org.apache.carbondata.sdk.file.Schema;

public class CarbonWriterHelper {

//    public static void CSVParseAndWrite(String inputPath, String outputDir, Field[] originalFields, String regex){
//        Field[] fields = originalFields;
//        System.out.println("Input file schema: ");
//        for(Field f:fields){
//            System.out.print("\t" + f.getFieldName() +" " +f.getDataType().toString());
//        }
//        System.out.print("\n");
//        if (!ProcessingContext.context.isEmpty()){
//            System.out.println("Sub-column schema transformation: ");
//            for (int i=fields.length-1; i>=0; i--){
//                if (ProcessingContext.context.containsKey(fields[i].getFieldName())){
//                    String[] curMeta = ProcessingContext.context.get(fields[i].getFieldName());
//                    SubAttConvertor curConv = new SubAttConvertor(fields[i].getFieldName(), curMeta[0],curMeta[2],curMeta[1], fields[i].getDataType(),(curMeta.length==4?curMeta[3]:null));
//                    ProcessingContext.merge.put(i,curConv);
//                    Field[] subFields = new Field[curConv.getNumSubCols()];
//
//                    for(int j=0;j<curConv.getNumSubCols();j++){
//                        subFields[j] = new Field(curConv.getSubcol()[j],curConv.getSubTypes()[j]);
//                    }
//                    fields = mergeArr(fields,i,subFields);
//                }
//            }
//            System.out.println("Translated file schema: ");
//            for(Field f:fields){
//                System.out.print("\t" + f.getFieldName() +" " +f.getDataType().toString());
//            }
//
//        }
//
//        try {
//            FileUtils.deleteDirectory(new File(outputDir));
//            BufferedReader reader = new BufferedReader(new FileReader(new File(inputPath)));
//            Map<String, String> map = new HashMap<String, String>();
//            map.put("complex_delimiter_level_1", "#");
//            CarbonWriter writer = CarbonWriter.builder()
//                    .outputPath(outputDir)
//                    .withLoadOptions(map)
//                    .buildWriterForCSVInput(new Schema(fields));
//            String line;
//            boolean skipHeader = true;
//            // Skip header line
//            line = skipHeader ? reader.readLine() : null;
//
//            if (ProcessingContext.merge.isEmpty()){
//                while ((line = reader.readLine()) != null) {
//                    // Handle the empty entry for each line
//                    String[] dataArray = parseByRegex(line,regex,fields.length);
//                    writer.write(dataArray);
//                }
//
//                reader.close();
//                writer.close();
//            }
//            else{
//                while ((line = reader.readLine()) != null) {
//                    // Handle the empty entry for each line
//                    String[] dataArray = parseByRegex(line,regex,originalFields.length);
//                    for (Map.Entry<Integer, SubAttConvertor> entry : ProcessingContext.merge.entrySet()) {
//                        int key = entry.getKey();
//                        SubAttConvertor curConv = entry.getValue();
//                        String[] subArr = curConv.parseByRegex(dataArray[key]);
//                        dataArray = mergeArr(dataArray, key, subArr);
//                    }
//                    writer.write(dataArray);
//                }
//
//                reader.close();
//                writer.close();
//
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//            System.out.println(e.getMessage());
//        } catch (InvalidLoadOptionException e) {
//            e.printStackTrace();
//            System.out.println(e.getMessage());
//        }
//
//    }


}