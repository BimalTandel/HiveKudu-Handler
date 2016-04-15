package org.kududb.mapred;

/**
 * Created by bimal on 4/13/16.
 */
import org.apache.hadoop.hive.kududb.KuduHandler.HiveKuduWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.kududb.Schema;
import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;
import org.kududb.client.*;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HiveKuduTableOutputFormat implements OutputFormat, Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(HiveKuduTableOutputFormat.class);

    /** Job parameter that specifies the output table. */
    static final String OUTPUT_TABLE_KEY = "kudu.mapreduce.output.table";

    /** Job parameter that specifies where the masters are */
    static final String MASTER_ADDRESSES_KEY = "kudu.mapreduce.master.addresses";

    /** Job parameter that specifies how long we wait for operations to complete */
    static final String OPERATION_TIMEOUT_MS_KEY = "kudu.mapreduce.operation.timeout.ms";

    /** Number of rows that are buffered before flushing to the tablet server */
    static final String BUFFER_ROW_COUNT_KEY = "kudu.mapreduce.buffer.row.count";

    /**
     * Job parameter that specifies which key is to be used to reach the HiveKuduTableOutputFormat
     * belonging to the caller
     */
    static final String MULTITON_KEY = "kudu.mapreduce.multitonkey";

    /**
     * This multiton is used so that the tasks using this output format/record writer can find
     * their KuduTable without having a direct dependency on this class,
     * with the additional complexity that the output format cannot be shared between threads.
     */
    private static final ConcurrentHashMap<String, HiveKuduTableOutputFormat> MULTITON = new
            ConcurrentHashMap<String, HiveKuduTableOutputFormat>();

    /**
     * This counter helps indicate which task log to look at since rows that weren't applied will
     * increment this counter.
     */
    public enum Counters { ROWS_WITH_ERRORS }

    private Configuration conf = null;

    private KuduClient client;
    private KuduTable table;
    private KuduSession session;
    private long operationTimeoutMs;

    @Override
    public void setConf(Configuration entries) {
        LOG.warn("I was called : setConf");
        this.conf = new Configuration(entries);

        String masterAddress = this.conf.get(MASTER_ADDRESSES_KEY);
        String tableName = this.conf.get(OUTPUT_TABLE_KEY);
        this.operationTimeoutMs = this.conf.getLong(OPERATION_TIMEOUT_MS_KEY,
                AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS);
        int bufferSpace = this.conf.getInt(BUFFER_ROW_COUNT_KEY, 1000);

        LOG.warn(" the master address here is " + masterAddress);

        this.client = new KuduClient.KuduClientBuilder(masterAddress)
                .defaultOperationTimeoutMs(operationTimeoutMs)
                .build();
        try {
            this.table = client.openTable(tableName);
        } catch (Exception ex) {
            throw new RuntimeException("Could not obtain the table from the master, " +
                    "is the master running and is this table created? tablename=" + tableName + " and " +
                    "master address= " + masterAddress, ex);
        }
        this.session = client.newSession();
        this.session.setFlushMode(AsyncKuduSession.FlushMode.AUTO_FLUSH_BACKGROUND);
        this.session.setMutationBufferSpace(bufferSpace);
        this.session.setIgnoreAllDuplicateRows(true);
        String multitonKey = String.valueOf(Thread.currentThread().getId());
        assert(MULTITON.get(multitonKey) == null);
        MULTITON.put(multitonKey, this);
        entries.set(MULTITON_KEY, multitonKey);
    }

    private void shutdownClient() throws IOException {
        LOG.warn("I was called : shutdownClient");
        try {
            client.shutdown();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public static KuduTable getKuduTable(String multitonKey) {
        LOG.warn("I was called : getKuduTable");
        return MULTITON.get(multitonKey).getKuduTable();
    }

    private KuduTable getKuduTable() {
        LOG.warn("I was called : getKuduTable");
        return this.table;
    }

    @Override
    public Configuration getConf() {
        LOG.warn("I was called : getConf");
        return conf;
    }


    @Override
    public RecordWriter getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable)
            throws IOException {
        LOG.warn("I was called : getRecordWriter");
        return new TableRecordWriter(this.session);
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
        LOG.warn("I was called : checkOutputSpecs");
        shutdownClient();
    }

    /*
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws
            IOException, InterruptedException {
        return new KuduTableOutputCommitter();
    }
    */

    protected class TableRecordWriter implements RecordWriter<NullWritable, HiveKuduWritable> {

        private final AtomicLong rowsWithErrors = new AtomicLong();
        private final KuduSession session;

        public TableRecordWriter(KuduSession session) {
            LOG.warn("I was called : TableRecordWriter");
            this.session = session;
        }

        private Operation getOperation(HiveKuduWritable hiveKuduWritable)
            throws IOException{
            LOG.warn("I was called : getOperation");
            int recCount = hiveKuduWritable.getColCount();
            Schema schema = table.getSchema();
            int colCount = schema.getColumnCount();
            if (recCount != colCount) {
                throw new IOException("Kudu table column count of " + colCount + " does not match "
                        + "with Serialized object record count of " + recCount);
            }
            //TODO: Find out if the record needs to be updated or deleted.
            //Assume only insert

            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();

            for (int i = 0; i < recCount; i++) {
                Object obj = hiveKuduWritable.get(i);
                LOG.warn("From Writable Column value of " + i + " is " + obj.toString() + " and type is " + hiveKuduWritable.getType(i).name());
                LOG.warn("From Schema Column name of " + i + " is " + schema.getColumnByIndex(i).getName());
                switch(hiveKuduWritable.getType(i)) {
                    case STRING: {
                        LOG.warn("I was called : STRING");
                        String s = obj.toString();
                        row.addString(i, s);
                        break;
                    }
                    case FLOAT: {
                        LOG.warn("I was called : FLOAT");
                        Float f = (Float) obj;
                        row.addFloat(i, f);
                        break;
                    }
                    case DOUBLE: {
                        LOG.warn("I was called : DOUBLE");
                        Double d = (Double) obj;
                        row.addDouble(i, d);
                        break;
                    }
                    case BOOL: {
                        LOG.warn("I was called : BOOL");
                        Boolean b = (Boolean) obj;
                        row.addBoolean(i, b);
                        break;
                    }
                    case INT8: {
                        LOG.warn("I was called : INT8");
                        Byte b = (Byte) obj;
                        row.addByte(i, b);
                        break;
                    }
                    case INT16: {
                        LOG.warn("I was called : INT16");
                        Short s = (Short) obj;
                        row.addShort(i, s);
                        break;
                    }
                    case INT32: {
                        LOG.warn("I was called : INT32");
                        Integer x = (Integer) obj;
                        row.addInt(i, x);
                        break;
                    }
                    case INT64: {
                        LOG.warn("I was called : INT64");
                        Long l = (Long) obj;
                        row.addLong(i, l);
                        break;
                    }
                    case TIMESTAMP: {
                        LOG.warn("I was called : TIMESTAMP");
                        Long time = (Long) obj;
                        row.addLong(i, time);
                        break;
                    }
                    case BINARY: {
                        LOG.warn("I was called : BINARY");
                        byte[] b = (byte[]) obj;
                        row.addBinary(i, b);
                        break;
                    }
                    default:
                        throw new IOException("Cannot write Object '"
                                + obj.getClass().getSimpleName() + "' as type: " + hiveKuduWritable.getType(i).name());
                }
            }

            return insert;
        }
        @Override
        public void write(NullWritable key, HiveKuduWritable kw)
                throws IOException {
            try {
                LOG.warn("I was called : write");
                Operation operation = getOperation(kw);
                session.apply(operation);

                //read from Kudu if the insert was successful
                List<String> projectColumns = new ArrayList<>(2);
                projectColumns.add("id");
                projectColumns.add("name");
                KuduScanner scanner = client.newScannerBuilder(table)
                        .setProjectedColumnNames(projectColumns)
                        .build();

                while (scanner.hasMoreRows()) {
                    RowResultIterator results = scanner.nextRows();
                    while (results.hasNext()) {
                        RowResult result = results.next();
                        LOG.warn("Returned from kudu" + result.getInt(0) + ":" +result.getString(1));
                    }
                }

                LOG.warn("applying operation");

            } catch (Exception e) {
                throw new IOException("Encountered an error while writing", e);
            }
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            try {
                LOG.warn("I was called : close");
                processRowErrors(session.close());
                shutdownClient();
            } catch (Exception e) {
                throw new IOException("Encountered an error while closing this task", e);
            } finally {
                if (reporter != null) {
                    // This is the only place where we have access to the context in the record writer,
                    // so set the counter here.
                    reporter.getCounter(Counters.ROWS_WITH_ERRORS).setValue(rowsWithErrors.get());
                }
            }
        }

        private void processRowErrors(List<OperationResponse> responses) {
            LOG.warn("I was called : processRowErrors");
            List<RowError> errors = OperationResponse.collectErrors(responses);
            if (!errors.isEmpty()) {
                int rowErrorsCount = errors.size();
                rowsWithErrors.addAndGet(rowErrorsCount);
                LOG.warn("Got per errors for " + rowErrorsCount + " rows, " +
                        "the first one being " + errors.get(0).getStatus());
            }
        }
    }
}