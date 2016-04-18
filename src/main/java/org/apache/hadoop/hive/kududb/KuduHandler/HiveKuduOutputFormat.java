package org.apache.hadoop.hive.kududb.KuduHandler;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.Insert;
import org.kududb.client.KuduTable;
import org.kududb.client.Operation;
import org.kududb.client.PartialRow;
import org.kududb.mapred.KuduTableOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Created by bimal on 4/17/16.
 */
public class HiveKuduOutputFormat implements OutputFormat<NullWritable, HiveKuduSerdeRow>,
        AcidOutputFormat<NullWritable, HiveKuduSerdeRow>, Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(HiveKuduOutputFormat.class);

    private KuduTableOutputFormat kuduOutputFormat;

    public HiveKuduOutputFormat() {
        kuduOutputFormat = new KuduTableOutputFormat();
    }

    @Override
    public void setConf(Configuration configuration) {
        kuduOutputFormat.setConf(configuration);
    }

    @Override
    public Configuration getConf() {
        return kuduOutputFormat.getConf();
    }

    @Override
    public RecordUpdater getRecordUpdater(Path path, Options options) throws IOException {
        return new HiveKuduRecordWriter(path, options);
    }

    @Override
    public FileSinkOperator.RecordWriter getRawRecordWriter(Path path, Options options) throws IOException {
        return null;
    }

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jobConf, Path path, Class aClass, boolean b, Properties properties, Progressable progressable) throws IOException {
        return null;
    }

    @Override
    public RecordWriter<NullWritable, HiveKuduSerdeRow> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
        return new HiveKuduRecordWriter(fileSystem, jobConf, s, progressable);
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
        kuduOutputFormat.checkOutputSpecs(fileSystem, jobConf);
    }

    protected class HiveKuduRecordWriter implements RecordWriter<NullWritable, HiveKuduSerdeRow>, RecordUpdater {

        private RecordWriter<NullWritable, Operation> tableWriter;
        private Options options;

        public HiveKuduRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
            tableWriter = kuduOutputFormat.getRecordWriter(fileSystem, jobConf, s, progressable);
        }

        public HiveKuduRecordWriter(Path p, Options o) throws IOException {
            options = o;
            //TODO: How to get jobconf, String and progressable from Options.
            tableWriter = kuduOutputFormat.getRecordWriter(p.getFileSystem(o.getConfiguration()), null, null, null);
        }

        @Override
        public void write(NullWritable nullWritable, HiveKuduSerdeRow hiveKuduSerdeRow) throws IOException {
            //TODO: What is the multitonkey?
            Operation operation = getOperation(hiveKuduSerdeRow, kuduOutputFormat.getKuduTable(null), HiveKuduSerdeRow.OperationType.INSERT);
            tableWriter.write(nullWritable, operation);
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            tableWriter.close(reporter);
        }

        @Override
        public void insert(long l, Object o) throws IOException {
            try {
                HiveKuduSerdeRow hiveKuduSerdeRow = new HiveKuduSerdeRow(o, options.getInspector());
                    Operation operation = getOperation(hiveKuduSerdeRow, kuduOutputFormat.getKuduTable(), HiveKuduSerdeRow.OperationType.INSERT);
                tableWriter.write(NullWritable.get(), operation);
            } catch (SerDeException sde) {
                throw new IOException(sde);
            }
        }

        @Override
        public void update(long l, Object o) throws IOException {
            try {
                HiveKuduSerdeRow hiveKuduSerdeRow = new HiveKuduSerdeRow(o, options.getInspector());
                Operation operation = getOperation(hiveKuduSerdeRow, kuduOutputFormat.getKuduTable(), HiveKuduSerdeRow.OperationType.UPDATE);
                tableWriter.write(NullWritable.get(), operation);
            } catch (SerDeException sde) {
                throw new IOException(sde);
            }
        }

        @Override
        public void delete(long l, Object o) throws IOException {
            try {
                HiveKuduSerdeRow hiveKuduSerdeRow = new HiveKuduSerdeRow(o, options.getInspector());
                Operation operation = getOperation(hiveKuduSerdeRow, kuduOutputFormat.getKuduTable(), HiveKuduSerdeRow.OperationType.DELETE);
                tableWriter.write(NullWritable.get(), operation);
            } catch (SerDeException sde) {
                throw new IOException(sde);
            }
        }

        @Override
        public void flush() throws IOException {

        }

        @Override
        public void close(boolean b) throws IOException {
            tableWriter.close(options.getReporter());
        }

        @Override
        public SerDeStats getStats() {
            return null;
        }
    }

    private Operation getOperation(HiveKuduSerdeRow hiveKuduSerdeRow, KuduTable table, HiveKuduSerdeRow.OperationType operationType)
            throws IOException{
        LOG.warn("I was called : getOperation");

        ObjectInspector inspector = hiveKuduSerdeRow.getInspector();
        Object row = hiveKuduSerdeRow.getRow();
        Type[] types = hiveKuduSerdeRow.getTypes();
        Object[] columnValues = new Object[types.length];

        final StructObjectInspector structInspector = (StructObjectInspector) inspector;
        final List<? extends StructField> fields = structInspector.getAllStructFieldRefs();

        if (fields.size() != hiveKuduSerdeRow.getTypes().length) {
            throw new IOException("Required " + hiveKuduSerdeRow.getTypes().length + " columns, received %d." +
                    fields.size());
        }

        for (int i = 0; i < types.length; i++) {
            StructField structField = fields.get(i);
            if (structField != null) {
                Object field = structInspector.getStructFieldData(row,
                        structField);
                ObjectInspector fieldOI = structField.getFieldObjectInspector();

                Object javaObject = HiveKuduBridgeUtils.deparseObject(field, fieldOI);
                LOG.warn("Column value of " + i + " is " + javaObject.toString());
                columnValues[i] = javaObject;
            }
        }

        int recCount = types.length;
        Schema schema = table.getSchema();
        int colCount = schema.getColumnCount();
        if (recCount != colCount) {
            throw new IOException("Kudu table column count of " + colCount + " does not match "
                    + "with Serialized object record count of " + recCount);
        }
        //TODO: Find out if the record needs to be updated or deleted.
        //Assume only insert

        Insert insert = table.newInsert();
        PartialRow kuduRow = insert.getRow();

        for (int i = 0; i < recCount; i++) {
            Object obj = columnValues[i];
            LOG.info("From Writable Column value of " + i + " is " + obj.toString() + " and type is " + types[i].name());
            LOG.info("From Schema Column name of " + i + " is " + schema.getColumnByIndex(i).getName());
            switch(types[i]) {
                case STRING: {
                    LOG.warn("I was called : STRING");
                    String s = obj.toString();
                    kuduRow.addString(i, s);
                    break;
                }
                case FLOAT: {
                    LOG.warn("I was called : FLOAT");
                    Float f = (Float) obj;
                    kuduRow.addFloat(i, f);
                    break;
                }
                case DOUBLE: {
                    LOG.warn("I was called : DOUBLE");
                    Double d = (Double) obj;
                    kuduRow.addDouble(i, d);
                    break;
                }
                case BOOL: {
                    LOG.warn("I was called : BOOL");
                    Boolean b = (Boolean) obj;
                    kuduRow.addBoolean(i, b);
                    break;
                }
                case INT8: {
                    LOG.warn("I was called : INT8");
                    Byte b = (Byte) obj;
                    kuduRow.addByte(i, b);
                    break;
                }
                case INT16: {
                    LOG.warn("I was called : INT16");
                    Short s = (Short) obj;
                    kuduRow.addShort(i, s);
                    break;
                }
                case INT32: {
                    LOG.warn("I was called : INT32");
                    Integer x = (Integer) obj;
                    kuduRow.addInt(i, x);
                    break;
                }
                case INT64: {
                    LOG.warn("I was called : INT64");
                    Long l = (Long) obj;
                    kuduRow.addLong(i, l);
                    break;
                }
                case TIMESTAMP: {
                    LOG.warn("I was called : TIMESTAMP");
                    Long time = (Long) obj;
                    kuduRow.addLong(i, time);
                    break;
                }
                case BINARY: {
                    LOG.warn("I was called : BINARY");
                    byte[] b = (byte[]) obj;
                    kuduRow.addBinary(i, b);
                    break;
                }
                default:
                    throw new IOException("Cannot write Object '"
                            + obj.getClass().getSimpleName() + "' as type: " + types[i].name());
            }
        }

        return insert;
    }
}
