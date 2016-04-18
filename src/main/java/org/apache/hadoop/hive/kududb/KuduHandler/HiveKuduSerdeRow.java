package org.apache.hadoop.hive.kududb.KuduHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.kududb.Type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by bimal on 4/17/16.
 */
public class HiveKuduSerdeRow implements Writable {
    private Object row;
    private ObjectInspector objectInspector;
    private Type[] types;

    public enum OperationType {
        INSERT, UPDATE, DELETE;
    }

    private static final Log LOG = LogFactory.getLog(HiveKuduSerdeRow.class);

    public HiveKuduSerdeRow(Object r, ObjectInspector oI, Type[] t) {
        row = r;
        objectInspector = oI;
        types = t;
    }

    public HiveKuduSerdeRow(Object r, ObjectInspector oI) throws SerDeException {
        row = r;
        objectInspector = oI;
        final StructObjectInspector structInspector = (StructObjectInspector) oI;
        final List<? extends StructField> fields = structInspector.getAllStructFieldRefs();

        types = new Type[fields.size()];

        for (int i = 0; i < fields.size(); i++) {
            types[i] = HiveKuduBridgeUtils.hiveTypeToKuduType(fields.get(i).getFieldObjectInspector().getTypeName());
        }
    }

    public Object getRow() {
        return row;
    }

    public ObjectInspector getInspector() {
        return objectInspector;
    }

    public Type[] getTypes() {
        return types;
    }

    /*
    //serialize
    final StructObjectInspector structInspector = (StructObjectInspector) inspector;
    final List<? extends StructField> fields = structInspector.getAllStructFieldRefs();


    if (fields.size() != fieldCount) {
        throw new SerDeException(String.format(
                "Required %d columns, received %d.", fieldCount,
                fields.size()));
    }

    cachedWritable.clear();

    for (int i = 0; i < fieldCount; i++) {
        StructField structField = fields.get(i);
        if (structField != null) {
            Object field = structInspector.getStructFieldData(row,
                    structField);
            ObjectInspector fieldOI = structField.getFieldObjectInspector();

            Object javaObject = new String(); //HiveKuduBridgeUtils.deparseObject(field, fieldOI);
            LOG.warn("Column value of " + i + " is " + javaObject.toString());
            cachedWritable.set(i, javaObject);
        }
    }

    //deserialize
    HiveKuduStruct tuple = (HiveKuduStruct) record;
        deserializeCache.clear();
        for (int i = 0; i < fieldCount; i++) {
            Object o = tuple.get(i);
            deserializeCache.add(o);
        }
    */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        throw new UnsupportedOperationException("can't write the bundle");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        throw new UnsupportedOperationException("can't read the bundle");
    }

    //return cachedWritable;
}
