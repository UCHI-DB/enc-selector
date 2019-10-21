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
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.commons.io.FileUtils;
//
import org.apache.carbondata.sdk.file.CarbonWriter;
import org.apache.carbondata.sdk.file.Field;
import org.apache.carbondata.sdk.file.Schema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CarbonWriterHelper {

    public static File genOutput(URI input, String suffix) {
        try {
            if (input.getPath().endsWith("\\.data")) {
                return new File(new URI(input.toString().replaceFirst("data$", suffix)));
            }
            return new File(new URI(input.toString() + "." + suffix));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static URI singleColumnString(URI input) throws IOException {
        File output = genOutput(input, "Carbon");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        Field[] fields = new Field[1];
        fields[0] = new Field("value", DataTypes.STRING);

        FileUtils.deleteDirectory(output);
        Map<String, String> map = new HashMap<String, String>();
        map.put("complex_delimiter_level_1", "#");
        CarbonWriter writer = null;
        try {
            writer = CarbonWriter.builder()
                    .outputPath(output.getAbsolutePath())
                    .withLoadOptions(map)
                    .buildWriterForCSVInput(new Schema(fields));
        } catch (InvalidLoadOptionException e) {
            e.printStackTrace();
        }
        String line;
        boolean skipHeader = true;
        // Skip header line
        line = skipHeader ? reader.readLine() : null;
        while ((line = reader.readLine()) != null) {
            // Handle the empty entry for each line
            String[] dataArray = new String[]{line.trim()};
            writer.write(dataArray);
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnBoolean(URI input) throws IOException {
        File output = genOutput(input, "Carbon");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        Field[] fields = new Field[1];
        fields[0] = new Field("value", DataTypes.BOOLEAN);

        FileUtils.deleteDirectory(output);
        Map<String, String> map = new HashMap<String, String>();
        map.put("complex_delimiter_level_1", "#");
        CarbonWriter writer = null;
        try {
            writer = CarbonWriter.builder()
                    .outputPath(output.getAbsolutePath())
                    .withLoadOptions(map)
                    .buildWriterForCSVInput(new Schema(fields));
        } catch (InvalidLoadOptionException e) {
            e.printStackTrace();
        }
        String line;
        boolean skipHeader = true;
        // Skip header line
        line = skipHeader ? reader.readLine() : null;
        while ((line = reader.readLine()) != null) {
            // Handle the empty entry for each line
            String[] dataArray = new String[]{line.trim()};
            writer.write(dataArray);
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnLong(URI input) throws IOException {
        File output = genOutput(input, "Carbon");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        Field[] fields = new Field[1];
        fields[0] = new Field("value", DataTypes.LONG);

        FileUtils.deleteDirectory(output);
        Map<String, String> map = new HashMap<String, String>();
        map.put("complex_delimiter_level_1", "#");
        CarbonWriter writer = null;
        try {
            writer = CarbonWriter.builder()
                    .outputPath(output.getAbsolutePath())
                    .withLoadOptions(map)
                    .buildWriterForCSVInput(new Schema(fields));
        } catch (InvalidLoadOptionException e) {
            e.printStackTrace();
        }
        String line;
        boolean skipHeader = true;
        // Skip header line
        line = skipHeader ? reader.readLine() : null;
        while ((line = reader.readLine()) != null) {
            // Handle the empty entry for each line
            String[] dataArray = new String[]{line.trim()};
            writer.write(dataArray);
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnInt(URI input) throws IOException {
        File output = genOutput(input, "Carbon");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        Field[] fields = new Field[1];
        fields[0] = new Field("value", DataTypes.INT);

        FileUtils.deleteDirectory(output);
        Map<String, String> map = new HashMap<String, String>();
        map.put("complex_delimiter_level_1", "#");
        CarbonWriter writer = null;
        try {
            writer = CarbonWriter.builder()
                    .outputPath(output.getAbsolutePath())
                    .withLoadOptions(map)
                    .buildWriterForCSVInput(new Schema(fields));
        } catch (InvalidLoadOptionException e) {
            e.printStackTrace();
        }
        String line;
        boolean skipHeader = true;
        // Skip header line
        line = skipHeader ? reader.readLine() : null;
        while ((line = reader.readLine()) != null) {
            // Handle the empty entry for each line
            String[] dataArray = new String[]{line.trim()};
            writer.write(dataArray);
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnFloat(URI input) throws IOException {
        File output = genOutput(input, "Carbon");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        Field[] fields = new Field[1];
        fields[0] = new Field("value", DataTypes.FLOAT);

        FileUtils.deleteDirectory(output);
        Map<String, String> map = new HashMap<String, String>();
        map.put("complex_delimiter_level_1", "#");
        CarbonWriter writer = null;
        try {
            writer = CarbonWriter.builder()
                    .outputPath(output.getAbsolutePath())
                    .withLoadOptions(map)
                    .buildWriterForCSVInput(new Schema(fields));
        } catch (InvalidLoadOptionException e) {
            e.printStackTrace();
        }
        String line;
        boolean skipHeader = true;
        // Skip header line
        line = skipHeader ? reader.readLine() : null;
        while ((line = reader.readLine()) != null) {
            // Handle the empty entry for each line
            String[] dataArray = new String[]{line.trim()};
            writer.write(dataArray);
        }

        reader.close();
        writer.close();

        return output.toURI();
    }


    public static URI singleColumnDouble(URI input) throws IOException {
        File output = genOutput(input, "Carbon");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        Field[] fields = new Field[1];
        fields[0] = new Field("value", DataTypes.DOUBLE);

        FileUtils.deleteDirectory(output);
        Map<String, String> map = new HashMap<String, String>();
        map.put("complex_delimiter_level_1", "#");
        CarbonWriter writer = null;
        try {
            writer = CarbonWriter.builder()
                    .outputPath(output.getAbsolutePath())
                    .withLoadOptions(map)
                    .buildWriterForCSVInput(new Schema(fields));
        } catch (InvalidLoadOptionException e) {
            e.printStackTrace();
        }
        String line;
        boolean skipHeader = true;
        // Skip header line
        line = skipHeader ? reader.readLine() : null;
        while ((line = reader.readLine()) != null) {
            // Handle the empty entry for each line
            String[] dataArray = new String[]{line.trim()};
            writer.write(dataArray);
        }

        reader.close();
        writer.close();

        return output.toURI();
    }


    public static void CSVParseAndWrite(String inputPath, String outputDir, Field[] originalFields, String regex){
        Field[] fields = originalFields;
        System.out.println("Input file schema: ");
        for(Field f:fields){
            System.out.print("\t" + f.getFieldName() +" " +f.getDataType().toString());
        }
        System.out.print("\n");
        if (!ProcessingContext.context.isEmpty()){
            System.out.println("Sub-column schema transformation: ");
            for (int i=fields.length-1; i>=0; i--){
                if (ProcessingContext.context.containsKey(fields[i].getFieldName())){
                    String[] curMeta = ProcessingContext.context.get(fields[i].getFieldName());
                    SubAttConvertor curConv = new SubAttConvertor(fields[i].getFieldName(), curMeta[0],curMeta[2],curMeta[1], fields[i].getDataType(),(curMeta.length==4?curMeta[3]:null));
                    ProcessingContext.merge.put(i,curConv);
                    Field[] subFields = new Field[curConv.getNumSubCols()];

                    for(int j=0;j<curConv.getNumSubCols();j++){
                        subFields[j] = new Field(curConv.getSubcol()[j],curConv.getSubTypes()[j]);
                    }
                    fields = mergeArr(fields,i,subFields);
                }
            }
            System.out.println("Translated file schema: ");
            for(Field f:fields){
                System.out.print("\t" + f.getFieldName() +" " +f.getDataType().toString());
            }

        }

        try {
            FileUtils.deleteDirectory(new File(outputDir));
            BufferedReader reader = new BufferedReader(new FileReader(new File(inputPath)));
            Map<String, String> map = new HashMap<String, String>();
            map.put("complex_delimiter_level_1", "#");
            CarbonWriter writer = CarbonWriter.builder()
                    .outputPath(outputDir)
                    .withLoadOptions(map)
                    .buildWriterForCSVInput(new Schema(fields));
            String line;
            boolean skipHeader = true;
            // Skip header line
            line = skipHeader ? reader.readLine() : null;

            if (ProcessingContext.merge.isEmpty()){
                while ((line = reader.readLine()) != null) {
                    // Handle the empty entry for each line
                    String[] dataArray = parseByRegex(line,regex,fields.length);
                    writer.write(dataArray);
                }

                reader.close();
                writer.close();
            }
            else{
                while ((line = reader.readLine()) != null) {
                    // Handle the empty entry for each line
                    String[] dataArray = parseByRegex(line,regex,originalFields.length);
                    for (Map.Entry<Integer, SubAttConvertor> entry : ProcessingContext.merge.entrySet()) {
                        int key = entry.getKey();
                        SubAttConvertor curConv = entry.getValue();
                        String[] subArr = curConv.parseByRegex(dataArray[key]);
                        dataArray = mergeArr(dataArray, key, subArr);
                    }
                    writer.write(dataArray);
                }

                reader.close();
                writer.close();

            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        } catch (InvalidLoadOptionException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
        }

    }

    public static void writeSubAttr2IndexFile(String indexFileName) throws IOException {
        Map<String, String[]> data = ProcessingContext.context;
        //System.out.println(data.toString()+ Arrays.toString(data.keySet().toArray()));

        // Convert Map to byte array
        FileOutputStream byteOut = new FileOutputStream(indexFileName,true);
        ObjectOutputStream out = new ObjectOutputStream(byteOut);
        out.writeObject(data);
        out.close();
    }

    public static String[] parseByRegex(String line, String regex, int cols){
        if (line != null){
            String[] arr = line.split(regex, -1);
            if (arr.length<cols){
                String[] ret = new String[cols];
                for (int i=0; i<arr.length; i++){
                    ret[i] = arr[i];
                }
                for (int i=arr.length; i<cols; i++){
                    ret[i] = "";
                }
                return ret;
            }
            else
                return arr;
        }
        return null;
    }

    public static String[] mergeArr (String[] line, int cols, String[] sub){
        String[] com = new String[line.length + sub.length-1];
        int i=0;
        for (; i< cols; i++){
            com[i] = line[i];
        }
        for (int j=0; j< sub.length; j++){
            com[cols+j] = sub[j];
        }
        i++;
        for (; i< line.length; i++){
            com[i+sub.length-1] = line[i];
        }
        return com;
    }

    public static Field[] mergeArr (Field[] line, int cols, Field[] sub){
        Field[] com = new Field[line.length + sub.length-1];
        int i=0;
        for (; i< cols; i++){
            com[i] = line[i];
        }
        for (int j=0; j< sub.length; j++){
            com[cols+j] = sub[j];
        }
        i++;
        for (; i< line.length; i++){
            com[i+sub.length-1] = line[i];
        }
        return com;
    }


}