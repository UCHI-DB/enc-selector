
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Chunwei Liu - initial API and implementation
 *
 */
package edu.uchicago.cs.encsel.adapter.carbondata;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.ConditionalExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.sdk.file.CarbonReader;
import org.apache.carbondata.sdk.file.CarbonSchemaReader;
import org.apache.carbondata.sdk.file.Schema;

import java.io.DataInputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;

public class CarbonReaderHelper {


    public static Object[] mergeCols(Object[] input, Map<Integer, SubAttConvertor> convertors){
        ArrayList<Object> ret = new ArrayList<Object>();
        for (int i=0;i<input.length;){
            if (convertors.containsKey(i)){
                SubAttConvertor conv = convertors.get(i);
                int cols = conv.getNumSubCols();
                String[] subCols = new String[cols];
                for (int j=0; j<cols;j++){
                    subCols[j]=String.valueOf(input[i]);
                    i++;
                }
                ret.add(conv.mergeIntoOne(subCols));
            }
            else {
                ret.add(input[i]);
                i++;
            }
        }
        Object[] retArr = new Object[ret.size()];
        return ret.toArray(retArr);
    }

    public static boolean isSubColFile(String indexFilePath) throws IOException {
        DataInputStream dataInputStream = FileFactory.getDataInputStream(indexFilePath, FileFactory.getFileType(indexFilePath));
        short s0 = dataInputStream.readShort();
        short s1 = dataInputStream.readShort();
        dataInputStream.close();
        if (s0 != (short)0xaced || s1 != (short)5) {
            return false;
        }
        return true;
    }

    public static Expression queryTranslation(Expression filterExpression){
        System.out.println("Original Query: " + filterExpression.getStatement());
        Objects.requireNonNull(filterExpression);
        if (null == filterExpression.getChildren())
            return filterExpression;
        else {
            if (filterExpression instanceof ConditionalExpression)
                return checkSubExp(filterExpression,null);
            else{
                Stack<Expression[]> nodeStack = new Stack<Expression[]>();
                for(Expression expression: filterExpression.getChildren()){
                    nodeStack.push(new Expression[]{expression,filterExpression} );
                }
                while (!nodeStack.isEmpty()) {
                    Expression[] curExp = nodeStack.pop();
                    if (curExp[0] instanceof ConditionalExpression){
                        checkSubExp(curExp[0],curExp[1]);
                    }
                    else if (curExp[0].getChildren() != null){
                        for(Expression expression : curExp[0].getChildren()){
                            nodeStack.push(new Expression[]{expression,curExp[0]} );
                        }
                    }
                }
                System.out.println("Query translated: " + filterExpression.getStatement());
                return filterExpression;
            }
        }
    }

    public static Expression checkSubExp(Expression exp, Expression parent){
        if (null != exp.getChildren()&&(exp instanceof ConditionalExpression)) {
            ColumnExpression colExp = null;
            Expression otherExp = null;
            boolean colLeft = true;
            int count = 0;
            for (Expression expression : exp.getChildren()) {
                if (expression instanceof ColumnExpression ){
                    if (ProcessingContext.context.containsKey(((ColumnExpression) expression).getColumnName())) {
                        colExp = (ColumnExpression) expression;
                        colLeft = (count == 0);
                    }
                } else
                    otherExp = expression;

                count++;
            }
            if (colExp != null) {
                LiteralExpression litExp = (LiteralExpression)otherExp;
                String[] subMeta = ProcessingContext.context.get(colExp.getColumnName());
                SubAttConvertor conv = new SubAttConvertor(colExp.getColumnName(), subMeta[0]);
                String[] subnames = subMeta[1].split(",", -1);
                String[] subvals = conv.parseByRegex((String) litExp.getLiteralExpValue());
                //System.out.println("literal value after split:" + Arrays.toString(subvals));
                DataType datatype = colExp.getDataType();
                ColumnExpression[] colExps = new ColumnExpression[subvals.length];
                LiteralExpression[] litExps = new LiteralExpression[subvals.length];
                Expression newExp = exp;
                if (exp instanceof EqualToExpression) {
                    colExps[0] = new ColumnExpression(subnames[0], datatype);
                    litExps[0] = new LiteralExpression(subvals[0], datatype);
                    newExp = colLeft ? new EqualToExpression(colExps[0], litExps[0]) : new EqualToExpression(litExps[0], colExps[0]);
                    for (int i = 1; i < subvals.length; i++) {
                        colExps[i] = new ColumnExpression(subnames[i], datatype);
                        litExps[i] = new LiteralExpression(subvals[i], datatype);
                        EqualToExpression temp = colLeft ? new EqualToExpression(colExps[i], litExps[i]) : new EqualToExpression(litExps[i], colExps[i]);
                        Expression left = newExp;
                        newExp = new AndExpression(left, temp);
                        //System.out.println("Query translated :" + newExp.getStatement());
                    }
                }

                if (parent == null) {
                    return newExp;
                } else {
                    //System.out.println("set query translated :" + newExp.getStatement());
                    parent.findAndSetChild(exp, newExp);
                    //System.out.println("set query translated :" + parent.getStatement());
                    return parent;
                }
            }
            return exp;
        }
        return null;
    }

    public static void readCarbonfile(String inputPath, String[] projection, Expression exp){
        try {
            File[] dataFiles = new File(inputPath).listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    if (name == null) {
                        return false;
                    }
                    return name.endsWith("carbonindex");
                }
            });
            if (dataFiles == null || dataFiles.length < 1) {
                throw new RuntimeException("Carbon index file not exists.");
            }
            Schema schema = CarbonSchemaReader
                    .readSchemaInIndexFile(dataFiles[0].getAbsolutePath())
                    .asOriginOrder();
            // Transform the schema
            String[] strings = new String[schema.getFields().length];
            for (int i = 0; i < schema.getFields().length; i++) {
                strings[i] = (schema.getFields())[i].getFieldName();
            }
            int pos = 3;
            String[] stringPos = new String[]{"State","ZIP","City","Address","Email"};
            //String[] stringPos = new String[]{"Domain","EmailID"};

            // Read data
            CarbonReader reader = null;
            if (projection!=null && exp!=null)
                reader= CarbonReader
                    .builder(inputPath, "_temp")
                    .projection(projection)
                    .filter(exp)
                    .build();
            else if(projection!=null)
                reader= CarbonReader
                        .builder(inputPath, "_temp")
                        .projection(projection)
                        .build();
            else if(exp!=null)
                reader= CarbonReader
                        .builder(inputPath, "_temp")
                        .filter(exp)
                        .build();
            else
                reader= CarbonReader
                        .builder(inputPath, "_temp")
                        .build();

            System.out.println("\nData:");
            long day = 24L * 3600 * 1000;
            int i = 0;
            while (reader.hasNext()) {

                Object[] row = (Object[]) reader.readNextRow();
                StringBuilder sb = new StringBuilder();
                sb.append(i);
                for(Object col : row){
                    sb.append("\t"+String.valueOf(col));
                }
                System.out.println(sb.toString());
                /*Object[] arr = (Object[]) row[4].toString().split("@");
                for (int j = 0; j < arr.length; j++) {
                    System.out.print(arr[j] + " ");
                }
                System.out.println();*/
                i++;
            }
            reader.close();
            ProcessingContext.offset=0;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
