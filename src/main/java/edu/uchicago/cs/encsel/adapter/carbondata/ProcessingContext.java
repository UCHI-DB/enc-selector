package edu.uchicago.cs.encsel.adapter.carbondata;

import org.apache.carbondata.format.DataType;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ProcessingContext {
    public static final Map<String, String[]> context = new HashMap<String, String[]>();

    public static final Map<String, DataType[]> subColType = new HashMap<String, DataType[]>();

    public static final Map<Integer, SubAttConvertor> merge = new LinkedHashMap<Integer, SubAttConvertor>();

    public static long offset = 0;

    public static final ThreadLocal<Map<String, String[]>> tContext = new ThreadLocal<Map<String, String[]>>() {
        @Override
        protected Map<String, String[]> initialValue() {
            return new HashMap<String, String[]>();
        }
    };


}
