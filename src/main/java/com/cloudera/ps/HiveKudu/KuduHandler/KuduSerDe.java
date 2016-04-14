package com.cloudera.ps.HiveKudu.KuduHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.kududb.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * Created by bimal on 4/12/16.
 */

public class KuduSerDe implements SerDe {

    private static final Log LOG = LogFactory.getLog(KuduSerDe.class);

    private KuduWritable cachedWritable; //Currently Update/Delete not supported from Hive.

    private int fieldCount;

    private StructObjectInspector objectInspector;
    private List<Object> deserializeCache;

    public KuduSerDe() {
    }

    @Override
    public void initialize(Configuration sysConf, Properties tblProps)
        throws SerDeException {

        LOG.debug("tblProps: " + tblProps);

        String columnNameProperty = tblProps
                .getProperty(Constants.LIST_COLUMNS);
        String columnTypeProperty = tblProps
                .getProperty(Constants.LIST_COLUMN_TYPES);

        if (columnNameProperty.length() == 0
                && columnTypeProperty.length() == 0) {
            //This is where we will implement option to connect to Kudu and get the column list using Serde.
        }

        List<String> columnNames = Arrays.asList(columnNameProperty.split(","));

        String[] columnTypes = columnTypeProperty.split(":");

        if (columnNames.size() != columnTypes.length) {
            throw new SerDeException("Splitting column and types failed." + "columnNames: "
                    + columnNames + ", columnTypes: "
                    + Arrays.toString(columnTypes));
        }

        final Type[] types = new Type[columnTypes.length];

        for (int i = 0; i < types.length; i++) {
            types[i] = HiveKuduBridgeUtils.hiveTypeToKuduType(columnTypes[i]);
        }

        this.cachedWritable = new KuduWritable(types);

        this.fieldCount = types.length;

        final List<ObjectInspector> fieldOIs = new ArrayList<>(columnTypes.length);

        for (int i = 0; i < types.length; i++) {
            ObjectInspector oi = HiveKuduBridgeUtils.getObjectInspector(types[i], columnTypes[i]);
            fieldOIs.add(oi);
        }

        this.objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, fieldOIs);

        this.deserializeCache = new ArrayList<>(columnTypes.length);

    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return KuduWritable.class;
    }

    @Override
    public KuduWritable serialize(Object row, ObjectInspector inspector)
        throws SerDeException {

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

                Object javaObject = HiveKuduBridgeUtils.deparseObject(field,
                        fieldOI);
                cachedWritable.set(i, javaObject);
            }
        }
        return cachedWritable;
    }

    @Override
    public Object deserialize(Writable record) throws SerDeException {
        if (!(record instanceof KuduWritable)) {
            throw new SerDeException("Expected KuduWritable, received "
                    + record.getClass().getName());
        }
        KuduWritable tuple = (KuduWritable) record;
        deserializeCache.clear();
        for (int i = 0; i < fieldCount; i++) {
            Object o = tuple.get(i);
            deserializeCache.add(o);
        }
        return deserializeCache;
    }

    @Override
    public SerDeStats getSerDeStats() {
        // TODO Auto-generated method stub
        return null;
    }
}


