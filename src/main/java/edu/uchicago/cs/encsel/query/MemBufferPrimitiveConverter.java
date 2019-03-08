package edu.uchicago.cs.encsel.query;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.List;

public class MemBufferPrimitiveConverter extends DictionaryPrimitiveConverter {

    private List<Object> buffer = new ArrayList<>();

    public List<Object> getBuffer() {
        return buffer;
    }

    public void setBuffer(List<Object> buffer) {
        this.buffer = buffer;
    }

    public MemBufferPrimitiveConverter(PrimitiveType type) {
        super(type);
    }

    @Override
    public void addBinary(Binary value) {
        buffer.add(value);
    }

    @Override
    public void addBoolean(boolean value) {
        buffer.add(value);
    }

    @Override
    public void addDouble(double value) {
        buffer.add(value);
    }

    @Override
    public void addFloat(float value) {
        buffer.add(value);
    }

    @Override
    public void addInt(int value) {
        buffer.add(value);
    }

    @Override
    public void addLong(long value) {
        buffer.add(value);
    }
}
