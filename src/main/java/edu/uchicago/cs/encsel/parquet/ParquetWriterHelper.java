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
package edu.uchicago.cs.encsel.parquet;

import edu.uchicago.cs.encsel.model.FloatEncoding;
import edu.uchicago.cs.encsel.model.IntEncoding;
import edu.uchicago.cs.encsel.model.LongEncoding;
import edu.uchicago.cs.encsel.model.StringEncoding;
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
import java.util.HashMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class ParquetWriterHelper {

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

    /**
     * Scan the file containing integer/long and determine the bit length
     *
     * @param input the file to scan
     * @return correct int bit length
     */
    public static int scanIntBitLength(URI input) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(input)));
            int maxBitLength = 0;
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty())
                    continue;
                int number = Integer.parseInt(line);
                int bitLength = 32 - Integer.numberOfLeadingZeros(number);
                if (bitLength > maxBitLength)
                    maxBitLength = bitLength;
            }
            br.close();
            return maxBitLength;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static int scanIntMaxInTab(URI input,int index) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(input)));
            int maxBitLength = 0;
            String line;
            String[] list;
            while ((line = br.readLine()) != null) {
                list = line.split("\\|");
                if (line.isEmpty())
                    continue;
                int number = Integer.parseInt(list[index]);
                if (number > maxBitLength)
                    //System.out.println(list.length);
                    maxBitLength = number;
            }
            br.close();
            return maxBitLength;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> String globalDict2Str(Object2IntMap<T> globDict) {
        StringBuilder ret = new StringBuilder();
        Iterator<T> objIterator = globDict.keySet().iterator();
        T firstKey = objIterator.next() ;
        if (firstKey instanceof Binary) {
            objIterator = globDict.keySet().iterator();
            Binary cur = (Binary)firstKey;
            while (objIterator.hasNext()) {
                cur = (Binary)objIterator.next();
                ret.append(cur.toStringUsingUTF8());
                ret.append("|");
            }
            ret.deleteCharAt(ret.length()-1);
            return ret.toString();
        }
        objIterator = globDict.keySet().iterator();
        while (objIterator.hasNext()){
            ret.append(objIterator.next().toString());
            ret.append("|");
        }
        ret.deleteCharAt(ret.length()-1);
        return ret.toString();
    }

    public static <T> Object2IntMap<T> buildGlobalDict(URI[] input, int[] index, MessageType[] schema, Boolean order, double backuprate) {
        try {
            String line;
            String[] list;
            PrimitiveTypeName typeName = schema[0].getColumns().get(index[0]).getType();
            System.out.println(typeName);
            Object2IntMap dictionaryContent = null;
            int i = 0;
            int idx = 0;
            switch (typeName) {
                case BINARY:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Binary>();
                    Set<Binary> treeSet = new TreeSet<Binary>();
                    if (!order)
                        treeSet = new LinkedHashSet<Binary>();
                    for (idx = 0; idx < input.length; idx++) {
                        BufferedReader br = new BufferedReader(new FileReader(new File(input[idx])));
                        while ((line = br.readLine()) != null) {
                            list = line.split("\\|");
                            if (line.isEmpty())
                                continue;
                            treeSet.add(Binary.fromString(list[index[idx]]));
                        }
                        br.close();
                    }
                    i = 0;
                    int id = 0;
                    int backup = (int) Math.floor(treeSet.size()*backuprate);
                    Set<Binary> backupset = new HashSet<Binary>();
                    for (Binary item : treeSet) {
                        if ((i>=200)&&(i<(200+backup))) {
                            backupset.add(item);
                        }
                        else{
                            dictionaryContent.put(item, id);
                            id++;
                        }
                        i++;
                    }
                    for (Binary bitem : backupset) {
                        dictionaryContent.put(bitem, id);
                        id++;
                    }
                    return dictionaryContent;
                case FIXED_LEN_BYTE_ARRAY:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Binary>();
                    Set<Binary> FtreeSet = new TreeSet<Binary>();
                    if (!order)
                        FtreeSet = new LinkedHashSet<Binary>();
                    for (idx = 0; idx < input.length; idx++) {
                        BufferedReader br = new BufferedReader(new FileReader(new File(input[idx])));
                        while ((line = br.readLine()) != null) {
                            list = line.split("\\|");
                            if (line.isEmpty())
                                continue;
                            FtreeSet.add(Binary.fromString(list[index[idx]]));
                        }
                        br.close();
                    }
                    i = 0;
                    for (Binary item : FtreeSet) {
                        dictionaryContent.put(item, i);
                        i++;
                    }
                    return dictionaryContent;
                case INT96:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Binary>();
                    Set<Binary> I96treeSet = new TreeSet<Binary>();
                    if (!order)
                        I96treeSet = new LinkedHashSet<Binary>();
                    for (idx = 0; idx < input.length; idx++) {
                        BufferedReader br = new BufferedReader(new FileReader(new File(input[idx])));
                        while ((line = br.readLine()) != null) {
                            list = line.split("\\|");
                            if (line.isEmpty())
                                continue;
                            I96treeSet.add(Binary.fromString(list[index[idx]]));
                        }
                        br.close();
                    }
                    i = 0;
                    for (Binary item : I96treeSet) {
                        dictionaryContent.put(item, i);
                        i++;
                    }
                    return dictionaryContent;
                case INT64:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Long>();
                    Set<Long> LongTreeSet = new TreeSet<Long>();
                    if (!order)
                        LongTreeSet = new LinkedHashSet<Long>();
                    for (idx = 0; idx < input.length; idx++) {
                        BufferedReader br = new BufferedReader(new FileReader(new File(input[idx])));
                        while ((line = br.readLine()) != null) {
                            list = line.split("\\|");
                            if (line.isEmpty())
                                continue;
                            LongTreeSet.add(Long.parseLong(list[index[idx]]));
                        }
                        br.close();
                    }
                    i = 0;
                    for (Long item : LongTreeSet) {
                        dictionaryContent.put(item, i);
                        i++;
                    }
                    return dictionaryContent;
                case DOUBLE:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Double>();
                    Set<Double> DoubleTreeSet = new TreeSet<Double>();
                    if (!order)
                        DoubleTreeSet = new LinkedHashSet<Double>();
                    for (idx = 0; idx < input.length; idx++) {
                        BufferedReader br = new BufferedReader(new FileReader(new File(input[idx])));
                        while ((line = br.readLine()) != null) {
                            list = line.split("\\|");
                            if (line.isEmpty())
                                continue;
                            DoubleTreeSet.add(Double.parseDouble(list[index[idx]]));
                        }
                        br.close();
                    }
                    i = 0;
                    for (Double item : DoubleTreeSet) {
                        dictionaryContent.put(item, i);
                        i++;
                    }
                    return dictionaryContent;
                case INT32:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Integer>();
                    Set<Integer> IntTreeSet = new TreeSet<Integer>();
                    if (!order)
                        IntTreeSet = new LinkedHashSet<Integer>();
                    for (idx = 0; idx < input.length; idx++) {
                        BufferedReader br = new BufferedReader(new FileReader(new File(input[idx])));
                        while ((line = br.readLine()) != null) {
                            list = line.split("\\|");
                            if (line.isEmpty())
                                continue;
                            IntTreeSet.add(Integer.parseInt(list[index[idx]]));
                        }
                        br.close();
                    }
                    i = 0;
                    for (Integer item : IntTreeSet) {
                        dictionaryContent.put(item, i);
                        i++;
                    }
                    return dictionaryContent;
                case FLOAT:
                    dictionaryContent = new Object2IntLinkedOpenHashMap<Float>();
                    Set<Float> FloatTreeSet = new TreeSet<Float>();
                    if (!order)
                        FloatTreeSet = new LinkedHashSet<Float>();
                    for (idx = 0; idx < input.length; idx++) {
                        BufferedReader br = new BufferedReader(new FileReader(new File(input[idx])));
                        while ((line = br.readLine()) != null) {
                            list = line.split("\\|");
                            if (line.isEmpty())
                                continue;
                            FloatTreeSet.add(Float.parseFloat(list[index[idx]]));
                        }
                        br.close();
                    }
                    i = 0;
                    for (Float item : FloatTreeSet) {
                        dictionaryContent.put(item, i);
                        i++;
                    }
                    return dictionaryContent;
                default:
                    throw new ParquetDecodingException("Dictionary encoding not supported for type: " + typeName);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> Object2IntMap<T> buildGlobalDict(URI input, int index, MessageType schema,Boolean order, double backuprate) {
        URI[] inputArr = {input};
        int[] indexArr = {index};
        MessageType[] schemaArr = {schema};
        return buildGlobalDict(inputArr,indexArr,schemaArr,order, backuprate);
    }

    public static int scanLongBitLength(URI input) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(input)));
            int maxBitLength = 0;
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty())
                    continue;
                long number = Long.parseLong(line);
                int bitLength = 64 - Long.numberOfLeadingZeros(number);
                if (bitLength > maxBitLength)
                    maxBitLength = bitLength;
            }
            br.close();
            return maxBitLength;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void write(URI input, MessageType schema, URI output, String split, boolean skipHeader) throws IOException {
        File outfile = new File(output);
        if (outfile.exists())
            outfile.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildForTable(new Path(output), schema);

        // Skip header line
        String line = skipHeader ? reader.readLine() : null;

        while ((line = reader.readLine()) != null) {
            String[] dataArray = line.trim().split(split);
            List<String> data = Arrays.asList(dataArray);
            writer.write(data);
        }

        reader.close();
        writer.close();
    }
    
    public static void write(URI input, MessageType schema, URI output, String split, boolean skipHeader, String compression) throws IOException {
        Profiler profiler = new Profiler();
        profiler.mark();
        
    		File outfile = new File(output);
        if (outfile.exists())
            outfile.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildForTableWithCompression(new Path(output), schema, compression);

        // Skip header line
        String line = skipHeader ? reader.readLine() : null;

        while ((line = reader.readLine()) != null) {
            String[] dataArray = line.trim().split(split);
            List<String> data = Arrays.asList(dataArray);
            writer.write(data);
        }

        reader.close();
        writer.close();
        
        profiler.pause();
        System.out.println(String.format("producing parquet file, %s,%d,%d,%d", compression, profiler.wcsum(), profiler.cpusum(),profiler.usersum()));
        
    }

    public static URI singleColumnBoolean(URI input) throws IOException {
        File output = genOutput(input, "PLAIN");
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BOOLEAN, "value"));

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildDefault(new Path(output.toURI()), schema);

        String line;
        List<String> holder = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            holder.add(line.trim());
            writer.write(holder);
            holder.clear();
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnInt(URI input, IntEncoding encoding) throws IOException {
        File output = genOutput(input, encoding.name());
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());
        int bitLength = scanIntBitLength(input);
        int bound = (1 << bitLength) - 1;
        EncContext.context.get().put(type, new Object[]{String.valueOf(bitLength), String.valueOf(bound)});

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildDefault(new Path(output.toURI()), schema);

        String line;
        List<String> holder = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            holder.add(line.trim());
            writer.write(holder);
            holder.clear();
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnLong(URI input, LongEncoding encoding) throws IOException {
        File output = genOutput(input, encoding.name());
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT64, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());
        int bitLength = scanLongBitLength(input);
        int bound = (1 << bitLength) - 1;
        EncContext.context.get().put(type, new Object[]{String.valueOf(bitLength), String.valueOf(bound)});

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildDefault(new Path(output.toURI()), schema);

        String line;
        List<String> holder = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            holder.add(line.trim());
            writer.write(holder);
            holder.clear();
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnString(URI input, StringEncoding encoding) throws IOException {
        File output = genOutput(input, encoding.name());
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildDefault(new Path(output.toURI()), schema);


        String line;
        List<String> holder = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            holder.add(line.trim());
            writer.write(holder);
            holder.clear();
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnDouble(URI input, FloatEncoding encoding) throws IOException {
        File output = genOutput(input, encoding.name());
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildDefault(new Path(output.toURI()), schema);

        String line;
        List<String> holder = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            holder.add(line.trim());
            writer.write(holder);
            holder.clear();
        }

        reader.close();
        writer.close();

        return output.toURI();
    }

    public static URI singleColumnFloat(URI input, FloatEncoding encoding) throws IOException {
        File output = genOutput(input, encoding.name());
        if (output.exists())
            output.delete();
        BufferedReader reader = new BufferedReader(new FileReader(new File(input)));

        MessageType schema = new MessageType("record",
                new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FLOAT, "value"));

        String type = schema.getColumns().get(0).toString();
        EncContext.encoding.get().put(type, encoding.parquetEncoding());

        ParquetWriter<List<String>> writer = ParquetWriterBuilder.buildDefault(new Path(output.toURI()), schema);

        String line;
        List<String> holder = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            holder.add(line.trim());
            writer.write(holder);
            holder.clear();
        }

        reader.close();
        writer.close();

        return output.toURI();
    }
}
