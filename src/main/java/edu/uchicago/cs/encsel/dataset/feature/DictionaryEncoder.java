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
 *     Hao Jiang - initial API and implementation
 *
 */
package edu.uchicago.cs.encsel.dataset.feature;

import edu.uchicago.cs.encsel.dataset.feature.BitOutputStream;
import edu.uchicago.cs.encsel.model.FloatEncoding;
import edu.uchicago.cs.encsel.model.IntEncoding;
import edu.uchicago.cs.encsel.model.LongEncoding;
import edu.uchicago.cs.encsel.model.StringEncoding;
import edu.uchicago.cs.encsel.parquet.EncContext;
import edu.uchicago.cs.encsel.parquet.ParquetWriterBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntLinkedOpenHashMap;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import edu.uchicago.cs.encsel.util.perf.Profiler;

import java.io.*;
import java.util.HashMap;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static edu.uchicago.cs.encsel.parquet.ParquetWriterHelper.genOutput;

public class DictionaryEncoder {

    public static URI genOutputURI(URI input, String suffix) {
        try {
            if (input.getPath().endsWith("\\.data")) {
                return new URI(input.toString().replaceFirst("data$", suffix));
            }
            return new URI(input.toString() + "." + suffix);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static void singleColumnBoolean(URI input, int batch) throws IOException {

    }

    /**
     *
     * @author Chunwei
     * @param input input file URI
     * @param batch number of value in each block, global mode when batch is Integer.MAX_VALUE
     * @throws IOException
     */
    public static void singleColumnInt(URI input, int batch) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(input.toString())));
        List<Integer> BPList = new ArrayList();
        // local dictionary encoding
        URI fUri = genOutputURI(input, "LDICTENCODING");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            fUri = genOutputURI(input, "GDICTENCODING");
        }
        int code = 0;
        File lOutput = new File(fUri.toString());
        if (lOutput.exists())
            lOutput.delete();
        DataOutputStream os = new DataOutputStream(new FileOutputStream(fUri.toString()));
        //local dictionary file
        URI dUri = genOutputURI(input, "LOCALDICT");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            dUri = genOutputURI(input, "GLOBALDICT");
        }
        File dOutput = new File(dUri.toString());
        if (dOutput.exists())
            dOutput.delete();
        DataOutputStream dictos = new DataOutputStream(new FileOutputStream(dUri.toString()));

        Map<Integer, Integer> dict =  new LinkedHashMap();
        String line;
        int cur = 0;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            cur = Integer.parseInt(line.trim());
            count++;
            if(dict.containsKey(cur))
                os.writeInt(dict.get(cur));
            else {
                os.writeInt(code);
                dict.put(cur,code);
                code++;
            }
            if(count==batch){
                dictos.writeInt(code);
                dictos.writeInt(count);
                dictos.writeInt(code*4);
                BPList.add(32-Integer.numberOfLeadingZeros(code-1));
                code = 0;
                for (Integer key : dict.keySet()){
                    dictos.writeInt(key);
                }
                count = 0;
                dict.clear();
            }
        }
        if(count!=0){
            dictos.writeInt(code);
            dictos.writeInt(count);
            dictos.writeInt(code*4);
            BPList.add(32-Integer.numberOfLeadingZeros(code-1));
            code = 0;
            for (Integer key : dict.keySet()){
                dictos.writeInt(key);
            }
            count = 0;
            dict.clear();
        }

        reader.close();
        os.close();
        dictos.close();

        DataInputStream is = new DataInputStream(new FileInputStream(fUri.toString()));
        // local dictionary encoding + BP
        URI gfUri = genOutputURI(input, "LBPDICTENCODING");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            gfUri = genOutputURI(input, "GBPDICTENCODING");
        }
        File glOutput = new File(gfUri.toString());
        if (glOutput.exists())
            glOutput.delete();
        BitOutputStream gos = new BitOutputStream(gfUri.toString());
        int encoded ;
        int listCount = 0;
        int bitToWrite;
        while (is.available()>0) {
            bitToWrite = BPList.get(listCount);
            encoded = is.readInt();
            //System.out.println(encoded +" " +bitToWrite );
            gos.write(bitToWrite, encoded);
            count++;
            if(count == batch){
                listCount++;
                count = 0;
            }
        }
        is.close();
        gos.close();
    }

    public static void singleColumnLong(URI input, int batch) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(input.toString())));
        List<Integer> BPList = new ArrayList();
        // local dictionary encoding
        URI fUri = genOutputURI(input, "LDICTENCODING");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            fUri = genOutputURI(input, "GDICTENCODING");
        }
        int code = 0;
        File lOutput = new File(fUri.toString());
        if (lOutput.exists())
            lOutput.delete();
        DataOutputStream os = new DataOutputStream(new FileOutputStream(fUri.toString()));
        //local dictionary file
        URI dUri = genOutputURI(input, "LOCALDICT");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            dUri = genOutputURI(input, "GLOBALDICT");
        }
        File dOutput = new File(dUri.toString());
        if (dOutput.exists())
            dOutput.delete();
        DataOutputStream dictos = new DataOutputStream(new FileOutputStream(dUri.toString()));

        Map<Long, Integer> dict =  new LinkedHashMap();
        String line;
        long cur = 0;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            cur = Long.parseLong(line.trim());
            count++;
            if(dict.containsKey(cur))
                os.writeInt(dict.get(cur));
            else {
                os.writeInt(code);
                dict.put(cur,code);
                code++;
            }
            if(count==batch){
                dictos.writeInt(code);
                dictos.writeInt(count);
                dictos.writeInt(code*8);
                BPList.add(32-Integer.numberOfLeadingZeros(code-1));
                code = 0;
                for (Long key : dict.keySet()){
                    dictos.writeLong(key);
                }
                count = 0;
                dict.clear();
            }
        }
        if(count!=0){
            dictos.writeInt(code);
            dictos.writeInt(count);
            dictos.writeInt(code*8);
            BPList.add(32-Integer.numberOfLeadingZeros(code-1));
            code = 0;
            for (Long key : dict.keySet()){
                dictos.writeLong(key);
            }
            count = 0;
            dict.clear();
        }

        reader.close();
        os.close();
        dictos.close();

        DataInputStream is = new DataInputStream(new FileInputStream(fUri.toString()));
        // local dictionary encoding + BP
        URI gfUri = genOutputURI(input, "LBPDICTENCODING");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            gfUri = genOutputURI(input, "GBPDICTENCODING");
        }
        File glOutput = new File(gfUri.toString());
        if (glOutput.exists())
            glOutput.delete();
        BitOutputStream gos = new BitOutputStream(gfUri.toString());
        int encoded ;
        int listCount = 0;
        int bitToWrite;
        while (is.available()>0) {
            bitToWrite = BPList.get(listCount);
            encoded = is.readInt();
            //System.out.println(encoded +" " +bitToWrite );
            gos.write(bitToWrite, encoded);
            count++;
            if(count == batch){
                listCount++;
                count = 0;
            }
        }
        is.close();
        gos.close();
    }

    public static void singleColumnString(URI input, int batch) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));
        List<Integer> BPList = new ArrayList();
        // local dictionary encoding
        URI fUri = genOutputURI(input, "LDICTENCODING");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            fUri = genOutputURI(input, "GDICTENCODING");
        }
        int code = 0;
        File lOutput = new File(fUri.toString());
        if (lOutput.exists())
            lOutput.delete();
        DataOutputStream os = new DataOutputStream(new FileOutputStream(fUri.toString()));
        //local dictionary file
        URI dUri = genOutputURI(input, "LOCALDICT");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            dUri = genOutputURI(input, "GLOBALDICT");
        }
        File dOutput = new File(dUri.toString());
        if (dOutput.exists())
            dOutput.delete();
        DataOutputStream dictos = new DataOutputStream(new FileOutputStream(dUri.toString()));

        Map<String, Integer> dict =  new LinkedHashMap();
        String line;
        String cur;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            cur = line.trim();
            count++;
            if(dict.containsKey(cur))
                os.writeInt(dict.get(cur));
            else {
                os.writeInt(code);
                dict.put(cur,code);
                code++;
            }
            if(count==batch){
                dictos.writeInt(code);
                dictos.writeInt(count);
                dictos.writeInt(-1);
                BPList.add(32-Integer.numberOfLeadingZeros(code-1));
                code = 0;
                for (String key : dict.keySet()){
                    dictos.writeUTF(key);
                }
                count = 0;
                dict.clear();
            }
        }
        if(count!=0){
            dictos.writeInt(code);
            dictos.writeInt(count);
            dictos.writeInt(-1);
            BPList.add(32-Integer.numberOfLeadingZeros(code-1));
            code = 0;
            for (String key : dict.keySet()){
                dictos.writeUTF(key);
            }
            count = 0;
            dict.clear();
        }

        reader.close();
        os.close();
        dictos.close();

        DataInputStream is = new DataInputStream(new FileInputStream(fUri.toString()));
        // local dictionary encoding + BP
        URI gfUri = genOutputURI(input, "LBPDICTENCODING");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            gfUri = genOutputURI(input, "GBPDICTENCODING");
        }
        File glOutput = new File(gfUri.toString());
        if (glOutput.exists())
            glOutput.delete();
        BitOutputStream gos = new BitOutputStream(gfUri.toString());
        int encoded ;
        int listCount = 0;
        int bitToWrite;
        while (is.available()>0) {
            bitToWrite = BPList.get(listCount);
            encoded = is.readInt();
            //System.out.println(encoded +" " +bitToWrite );
            gos.write(bitToWrite, encoded);
            count++;
            if(count == batch){
                listCount++;
                count = 0;
            }
        }
        is.close();
        gos.close();
    }

    public static void singleColumnDouble(URI input, int batch) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(input.toString())));
        List<Integer> BPList = new ArrayList();
        // local dictionary encoding
        URI fUri = genOutputURI(input, "LDICTENCODING");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            fUri = genOutputURI(input, "GDICTENCODING");
        }
        int code = 0;
        File lOutput = new File(fUri.toString());
        if (lOutput.exists())
            lOutput.delete();
        DataOutputStream os = new DataOutputStream(new FileOutputStream(fUri.toString()));
        //local dictionary file
        URI dUri = genOutputURI(input, "LOCALDICT");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            dUri = genOutputURI(input, "GLOBALDICT");
        }
        File dOutput = new File(dUri.toString());
        if (dOutput.exists())
            dOutput.delete();
        DataOutputStream dictos = new DataOutputStream(new FileOutputStream(dUri.toString()));

        Map<Double, Integer> dict =  new LinkedHashMap();
        String line;
        Double cur = 0.0;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            cur = Double.parseDouble(line.trim());
            count++;
            if(dict.containsKey(cur))
                os.writeInt(dict.get(cur));
            else {
                os.writeInt(code);
                dict.put(cur,code);
                code++;
            }
            if(count==batch){
                dictos.writeInt(code);
                dictos.writeInt(count);
                dictos.writeInt(code*8);
                BPList.add(32-Integer.numberOfLeadingZeros(code-1));
                code = 0;
                for (Double key : dict.keySet()){
                    dictos.writeDouble(key);
                }
                count = 0;
                dict.clear();
            }
        }
        if(count!=0){
            dictos.writeInt(code);
            dictos.writeInt(count);
            dictos.writeInt(code*8);
            BPList.add(32-Integer.numberOfLeadingZeros(code-1));
            code = 0;
            for (Double key : dict.keySet()){
                dictos.writeDouble(key);
            }
            count = 0;
            dict.clear();
        }

        reader.close();
        os.close();
        dictos.close();

        DataInputStream is = new DataInputStream(new FileInputStream(fUri.toString()));
        // local dictionary encoding + BP
        URI gfUri = genOutputURI(input, "LBPDICTENCODING");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            gfUri = genOutputURI(input, "GBPDICTENCODING");
        }
        File glOutput = new File(gfUri.toString());
        if (glOutput.exists())
            glOutput.delete();
        BitOutputStream gos = new BitOutputStream(gfUri.toString());
        int encoded ;
        int listCount = 0;
        int bitToWrite;
        while (is.available()>0) {
            bitToWrite = BPList.get(listCount);
            encoded = is.readInt();
            System.out.println(encoded +" " +bitToWrite );
            gos.write(bitToWrite, encoded);
            count++;
            if(count == batch){
                listCount++;
                count = 0;
            }
        }
        is.close();
        gos.close();
    }

    public static void singleColumnFloat(URI input, int batch) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(input.toString())));
        List<Integer> BPList = new ArrayList();
        // local dictionary encoding
        URI fUri = genOutputURI(input, "LDICTENCODING");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            fUri = genOutputURI(input, "GDICTENCODING");
        }
        int code = 0;
        File lOutput = new File(fUri.toString());
        if (lOutput.exists())
            lOutput.delete();
        DataOutputStream os = new DataOutputStream(new FileOutputStream(fUri.toString()));
        //local dictionary file
        URI dUri = genOutputURI(input, "LOCALDICT");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            dUri = genOutputURI(input, "GLOBALDICT");
        }
        File dOutput = new File(dUri.toString());
        if (dOutput.exists())
            dOutput.delete();
        DataOutputStream dictos = new DataOutputStream(new FileOutputStream(dUri.toString()));

        Map<Float, Integer> dict =  new LinkedHashMap();
        String line;
        float cur = 0;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            cur = Float.parseFloat(line.trim());
            count++;
            if(dict.containsKey(cur))
                os.writeInt(dict.get(cur));
            else {
                os.writeInt(code);
                dict.put(cur,code);
                code++;
            }
            if(count==batch){
                dictos.writeInt(code);
                dictos.writeInt(count);
                dictos.writeInt(code*4);
                BPList.add(32-Integer.numberOfLeadingZeros(code-1));
                code = 0;
                for (Float key : dict.keySet()){
                    dictos.writeFloat(key);
                }
                count = 0;
                dict.clear();
            }
        }
        if(count!=0){
            dictos.writeInt(code);
            dictos.writeInt(count);
            dictos.writeInt(code*4);
            BPList.add(32-Integer.numberOfLeadingZeros(code-1));
            code = 0;
            for (Float key : dict.keySet()){
                dictos.writeFloat(key);
            }
            count = 0;
            dict.clear();
        }

        reader.close();
        os.close();
        dictos.close();

        DataInputStream is = new DataInputStream(new FileInputStream(fUri.toString()));
        // local dictionary encoding + BP
        URI gfUri = genOutputURI(input, "LBPDICTENCODING");
        if (batch == Integer.MAX_VALUE) {
            // use global encoding
            gfUri = genOutputURI(input, "GBPDICTENCODING");
        }
        File glOutput = new File(gfUri.toString());
        if (glOutput.exists())
            glOutput.delete();
        BitOutputStream gos = new BitOutputStream(gfUri.toString());
        int encoded ;
        int listCount = 0;
        int bitToWrite;
        while (is.available()>0) {
            bitToWrite = BPList.get(listCount);
            encoded = is.readInt();
            System.out.println(encoded +" " +bitToWrite );
            gos.write(bitToWrite, encoded);
            count++;
            if(count == batch){
                listCount++;
                count = 0;
            }
        }
        is.close();
        gos.close();
    }

    public static void main(String[] args){
        try {
            singleColumnString(new URI("src/test/resource/coldata/test_col_int.data"),114);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


    }
}
