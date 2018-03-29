package edu.uchicago.cs.encsel.parquet;

import edu.uchicago.cs.encsel.util.word.Dict;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.it.unimi.dsi.fastutil.objects.Object2IntMap;

import java.util.HashMap;
import java.util.Map;

public class EncContext {
    public static final ThreadLocal<Map<String, Object[]>> context = new ThreadLocal<Map<String, Object[]>>() {
        @Override
        protected Map<String, Object[]> initialValue() {
            return new HashMap<String, Object[]>();
        }
    };
    public static final ThreadLocal<Map<String, Encoding>> encoding = new ThreadLocal<Map<String, Encoding>>() {
        @Override
        protected Map<String, Encoding> initialValue() {
            return new HashMap<String, Encoding>();
        }
    };

    public static final ThreadLocal<Map<String, Object2IntMap>> globalDict = new ThreadLocal<Map<String, Object2IntMap>>() {
        @Override
        protected Map<String, Object2IntMap> initialValue() {
            return new HashMap<String, Object2IntMap>();
        }
    };

    public static final ThreadLocal<Map<String, DictionaryPage>> dictPage = new ThreadLocal<Map<String, DictionaryPage>>() {
        @Override
        protected Map<String, DictionaryPage> initialValue() {
            return new HashMap<String, DictionaryPage>();
        }
    };
}
