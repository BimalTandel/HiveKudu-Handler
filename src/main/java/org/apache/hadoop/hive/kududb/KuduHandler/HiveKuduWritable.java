package org.apache.hadoop.hive.kududb.KuduHandler;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.kududb.Type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by bimal on 4/12/16.
 */
public class HiveKuduWritable implements Writable {


    private Object[] columnValues;
    private Type[] columnTypes;

    public HiveKuduWritable() {

    }

    public HiveKuduWritable(Type[] types) {
        this.columnValues = new Object[types.length];
        this.columnTypes = types;
    }

    public void clear() {
        Arrays.fill(columnValues, null);
    }

    public void set(int i, Object javaObject) {
        columnValues[i] = javaObject;
    }

    public Object get(int i) {
        return columnValues[i];
    }

    public Type getType(int i) { return columnTypes[i]; }

    public int getColCount() {
        return this.columnTypes.length;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        if (size == -1) {
            return;
        }
        if (columnValues == null) {
            this.columnValues = new Object[size];
            this.columnTypes = new Type[size];
        } else {
            clear();
        }
        for (int i = 0; i < size; i++) {
            Type kuduType = WritableUtils.readEnum(in, Type.class);
            columnTypes[i] = kuduType;
            Object v = HiveKuduBridgeUtils.readObject(in, kuduType);
            columnValues[i] = v;
        }
    }
    @Override
    public void write(DataOutput out) throws IOException {
        if (columnValues == null) {
            out.writeInt(-1);
            return;
        }
        if (columnTypes == null) {
            out.writeInt(-1);
            return;
        }

        final Object[] values = this.columnValues;
        final Type[] types = this.columnTypes;

        out.writeInt(values.length);

        for (int i = 0; i < values.length; i++) {
            HiveKuduBridgeUtils.writeObject(values[i], types[i], out);
        }
    }
}
