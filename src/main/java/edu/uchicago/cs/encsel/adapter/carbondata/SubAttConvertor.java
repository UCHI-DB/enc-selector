package edu.uchicago.cs.encsel.adapter.carbondata;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * \* initial API and implementation.
 * \* contributors: Chunwei Liu
 * \* Date: 11/6/2018
 * \* Time: 9:24 AM
 * \*
 * \
 */
public class SubAttConvertor {

    private String colName;
    private String regex;
    private String split;
    private Pattern pt;
    private int numSubCols;
    private String[] subcol;
    private DataType colType;
    private DataType[] subTypes;


    public SubAttConvertor(String name, String regex, String splits, String subCols, DataType colType, String subTypes) {
        this.colName = name;
        this.regex = regex;
        this.pt = Pattern.compile(regex);
        this.split = splits;
        this.colType = colType;
        this.numSubCols = splits.split(",",-1).length - 1;
        this.subTypes = new DataType[numSubCols];
        if (subTypes==null){
            for (int i=0;i<numSubCols;i++){
                this.subTypes[i] = this.colType;
            }
        }
        else {
            String[] subTypeStr = subTypes.split(",",-1);
            for (int i=0;i<numSubCols;i++){
                this.subTypes[i] = getType(subTypeStr[i]);
            }
        }
        if(subCols==null){
            String[] autoGenSubCol = new String[numSubCols];
            for (int i=0;i<numSubCols;i++){
                autoGenSubCol[i] = colName+"sub"+i;
            }
            this.subcol = autoGenSubCol;
        }
        else
            this.subcol = subCols.split(",",-1);
    }

    public static DataType getType(String str){
        String id = str.toUpperCase();
        if (id.equalsIgnoreCase("STRING")) {
            return DataTypes.STRING;
        } else if (id.equalsIgnoreCase("DATE")) {
            return DataTypes.DATE;
        } else if (id.equalsIgnoreCase("TIMESTAMP")) {
            return DataTypes.TIMESTAMP;
        } else if (id.equalsIgnoreCase("BOOLEAN")) {
            return DataTypes.BOOLEAN;
        } else if (id.equalsIgnoreCase("BYTE")) {
            return DataTypes.BYTE;
        } else if (id.equalsIgnoreCase("SHORT")) {
            return DataTypes.SHORT;
        } else if (id.equalsIgnoreCase("SHORT_INT")) {
            return DataTypes.SHORT_INT;
        } else if (id.equalsIgnoreCase("INT")) {
            return DataTypes.INT;
        } else if (id.equalsIgnoreCase("LONG")) {
            return DataTypes.LONG;
        } else if (id.equalsIgnoreCase("LEGACY_LONG")) {
            return DataTypes.LEGACY_LONG;
        } else if (id.equalsIgnoreCase("FLOAT")) {
            return DataTypes.FLOAT;
        } else if (id.equalsIgnoreCase("DOUBLE")) {
            return DataTypes.DOUBLE;
        } else if (id.equalsIgnoreCase("NULL")) {
            return DataTypes.NULL;
        } else if (id.equalsIgnoreCase("DECIMAL_TYPE_ID")) {
            return DataTypes.createDefaultDecimalType();
        } else if (id.equalsIgnoreCase("ARRAY_TYPE_ID")) {
            return DataTypes.createDefaultArrayType();
        } else if (id.equalsIgnoreCase("STRUCT_TYPE_ID")) {
            return DataTypes.createDefaultStructType();
        } else if (id.equalsIgnoreCase("MAP_TYPE_ID")) {
            return DataTypes.createDefaultMapType();
        } else if (id.equalsIgnoreCase("BYTE_ARRAY")) {
            return DataTypes.BYTE_ARRAY;
        } else if (id.equalsIgnoreCase("VARCHAR")) {
            return DataTypes.VARCHAR;
        } else {
            throw new RuntimeException("create DataType with invalid id: " + id);
        }

    }
    public SubAttConvertor(String name, String regex, String splits, String subCols){
        this(name, regex, splits, subCols,DataTypes.STRING,null);
    }

    public SubAttConvertor(String name, String regex) {
        this(name, regex, "", null,DataTypes.STRING,null);
    }

    public String[] parseByRegex(String record){
        ArrayList<String> ret = new ArrayList<String>();
        Matcher match = pt.matcher(record);
        while (match.find()){
            for (int i = 1; i<= match.groupCount(); i++)
            ret.add(match.group(i));
        }
        return ret.toArray(new String[ret.size()]);
    }

    public int getNumSubCols() {
        return numSubCols;
    }


    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }


    public String[] getSubcol() {
        return subcol;
    }


    public DataType getColType() {
        return colType;
    }


    public DataType[] getSubTypes() {
        return subTypes;
    }


    public String mergeIntoOne(String[] subcols){
        // restore the original column
        String[] splits = this.split.split(",", -1);
        StringBuffer sb = new StringBuffer();
        int i = 0;
        for (String col:subcols){
            sb.append(splits[i]);
            sb.append(col);
            i++;
        }
        sb.append(splits[i]);
        return sb.toString();
    }
}
