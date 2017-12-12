package edu.uchicago.cs.encsel.dataset.parquet;

import org.apache.parquet.column.Encoding;

import java.util.Map;

public class EncContext {
    public static final ThreadLocal<Map<String,Object[]>> context = new ThreadLocal<>();
    public static final ThreadLocal<Map<String, Encoding>> encoding = new ThreadLocal<>();
}
