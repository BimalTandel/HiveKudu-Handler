package org.kududb.mapred;

/**
 * Created by bimal on 4/13/16.
 */
import org.apache.hadoop.hive.kududb.KuduHandler.HiveKuduWritable;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.kudu.Type;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.kudu.Schema;
import org.apache.kudu.annotations.InterfaceAudience;
import org.apache.kudu.annotations.InterfaceStability;
import org.apache.kudu.client.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.net.DNS;

import javax.naming.NamingException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This input format generates one split per tablet and the only location for each split is that
 * tablet's leader.
 * </p>
 *
 * <p>
 * Hadoop doesn't have the concept of "closing" the input format so in order to release the
 * resources we assume that once either {@link #getSplits(org.apache.hadoop.mapred.JobConf, int)}
 * or {@link HiveKuduTableInputFormat.KuduTableRecordReader#close()} have been called that
 * the object won't be used again and the AsyncKuduClient is shut down.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HiveKuduTableInputFormat implements InputFormat, Configurable {

    private static final Log LOG = LogFactory.getLog(HiveKuduTableInputFormat.class);

    private static final long SLEEP_TIME_FOR_RETRIES_MS = 1000;

    /** Job parameter that specifies the input table. */
    static final String INPUT_TABLE_KEY = "kudu.mapreduce.input.table";

    /** Job parameter that specifies if the scanner should cache blocks or not (default: false). */
    static final String SCAN_CACHE_BLOCKS = "kudu.mapreduce.input.scan.cache.blocks";

    /** Job parameter that specifies where the masters are. */
    static final String MASTER_ADDRESSES_KEY = "kudu.mapreduce.master.addresses";

    /** Job parameter that specifies how long we wait for operations to complete (default: 10s). */
    static final String OPERATION_TIMEOUT_MS_KEY = "kudu.mapreduce.operation.timeout.ms";

    /** Job parameter that specifies the address for the name server. */
    static final String NAME_SERVER_KEY = "kudu.mapreduce.name.server";

    /** Job parameter that specifies the encoded column range predicates (may be empty). */
    static final String ENCODED_COLUMN_RANGE_PREDICATES_KEY =
            "kudu.mapreduce.encoded.column.range.predicates";

    /**
     * Job parameter that specifies the column projection as a comma-separated list of column names.
     *
     * Not specifying this at all (i.e. setting to null) or setting to the special string
     * '*' means to project all columns.
     *
     * Specifying the empty string means to project no columns (i.e just count the rows).
     */
    static final String COLUMN_PROJECTION_KEY = "kudu.mapreduce.column.projection";

    /**
     * The reverse DNS lookup cache mapping: address from Kudu => hostname for Hadoop. This cache is
     * used in order to not do DNS lookups multiple times for each tablet server.
     */
    private final Map<String, String> reverseDNSCacheMap = new HashMap<String, String>();

    private Configuration conf;
    private KuduClient client;
    private KuduTable table;
    private long operationTimeoutMs;
    private String nameServer;
    private boolean cacheBlocks;
    private List<String> projectedCols;
    private byte[] rawPredicates;
    private String kuduMasterAddress;
    
    private KuduClient makeKuduClient() {
    	return new KuduClient.KuduClientBuilder(this.kuduMasterAddress)
                .defaultOperationTimeoutMs(operationTimeoutMs)
                .build();
    }

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int i)
            throws IOException {
        
            if (table == null) {
                throw new IOException("No table was provided");
            }
            
            List<KuduScanToken> tokens = this.client.newScanTokenBuilder(table).build();
            KuduTableSplit[] splits = new KuduTableSplit[tokens.size()];
            for(int j=0; j<tokens.size(); ++j)
            	splits[j] = KuduTableSplit.of(tokens.get(j));
            return splits;
    }

    /**
     * This method might seem alien, but we do this in order to resolve the hostnames the same way
     * Hadoop does. This ensures we get locality if Kudu is running along MR/YARN.
     * @param host hostname we got from the master
     * @param port port we got from the master
     * @return reverse DNS'd address
     */
    private String reverseDNS(String host, Integer port) {
        LOG.warn("I was called : reverseDNS");
        String location = this.reverseDNSCacheMap.get(host);
        if (location != null) {
            return location;
        }
        // The below InetSocketAddress creation does a name resolution.
        InetSocketAddress isa = new InetSocketAddress(host, port);
        if (isa.isUnresolved()) {
            LOG.warn("Failed address resolve for: " + isa);
        }
        InetAddress tabletInetAddress = isa.getAddress();
        try {
            location = domainNamePointerToHostName(
                    DNS.reverseDns(tabletInetAddress, this.nameServer));
            this.reverseDNSCacheMap.put(host, location);
        } catch (NamingException e) {
            LOG.warn("Cannot resolve the host name for " + tabletInetAddress + " because of " + e);
            location = host;
        }
        return location;
    }

    @Override
    public RecordReader<NullWritable, HiveKuduWritable> getRecordReader(InputSplit inputSplit,
                                                                        final JobConf jobConf, final Reporter reporter)
            throws IOException {
    	KuduTableSplit tableSplit = (KuduTableSplit) inputSplit;
        LOG.warn("I was called : getRecordReader");
        try {
            return new KuduTableRecordReader(tableSplit, this.makeKuduClient());
        } catch (InterruptedException e)
        {
            throw new IOException(e);
        }
    }

    @Override
    public void setConf(Configuration entries) {
    	LOG.warn("I was called : setConf");
        this.conf = new Configuration(entries);

        String tableName = conf.get(INPUT_TABLE_KEY);
        this.kuduMasterAddress = conf.get(MASTER_ADDRESSES_KEY);
        this.operationTimeoutMs = conf.getLong(OPERATION_TIMEOUT_MS_KEY,
                AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS);
        this.nameServer = conf.get(NAME_SERVER_KEY);
        this.cacheBlocks = conf.getBoolean(SCAN_CACHE_BLOCKS, false);

        LOG.warn(" the master address here is " + this.kuduMasterAddress);

        this.client = this.makeKuduClient();
        try {
            this.table = client.openTable(tableName);
        } catch (Exception ex) {
            throw new RuntimeException("Could not obtain the table from the master, " +
                    "is the master running and is this table created? tablename=" + tableName + " and " +
                    "master address= " + this.kuduMasterAddress, ex);
        }

    }

    /**
     * Given a PTR string generated via reverse DNS lookup, return everything
     * except the trailing period. Example for host.example.com., return
     * host.example.com
     * @param dnPtr a domain name pointer (PTR) string.
     * @return Sanitized hostname with last period stripped off.
     *
     */
    private static String domainNamePointerToHostName(String dnPtr) {
        LOG.warn("I was called : domainNamePointerToHostName");
        if (dnPtr == null)
            return null;
        String r = dnPtr.endsWith(".") ? dnPtr.substring(0, dnPtr.length() - 1) : dnPtr;
        LOG.warn(r);
        return r;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    static class TableSplit implements InputSplit, Writable, Comparable<TableSplit> {

        private byte[] startPartitionKey;
        private byte[] endPartitionKey;
        private String[] locations;

        public TableSplit() { } // Writable

        public TableSplit(byte[] startPartitionKey, byte[] endPartitionKey, String[] locations) {
            LOG.warn("I was called : TableSplit");
            this.startPartitionKey = startPartitionKey;
            this.endPartitionKey = endPartitionKey;
            this.locations = locations;
        }

        @Override
        public long getLength() throws IOException {
            // TODO Guesstimate a size
            return 0;
        }

        @Override
        public String[] getLocations() throws IOException {
            LOG.warn("I was called : getLocations");
            return locations;
        }

        public byte[] getStartPartitionKey() {
            return startPartitionKey;
        }

        public byte[] getEndPartitionKey() {
            return endPartitionKey;
        }

        @Override
        public int compareTo(TableSplit tableSplit) {
            LOG.warn("I was called : compareTo");
            return Bytes.memcmp(startPartitionKey, tableSplit.getStartPartitionKey());
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            LOG.warn("I was called : write");
            Bytes.writeByteArray(dataOutput, startPartitionKey);
            Bytes.writeByteArray(dataOutput, endPartitionKey);
            dataOutput.writeInt(locations.length);
            for (String location : locations) {
                byte[] str = Bytes.fromString(location);
                Bytes.writeByteArray(dataOutput, str);
            }
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            LOG.warn("I was called : readFields");
            startPartitionKey = Bytes.readByteArray(dataInput);
            endPartitionKey = Bytes.readByteArray(dataInput);
            locations = new String[dataInput.readInt()];
            LOG.warn("readFields " + locations.length);
            for (int i = 0; i < locations.length; i++) {
                byte[] str = Bytes.readByteArray(dataInput);
                locations[i] = Bytes.getString(str);
                LOG.warn("readFields " + locations[i]);
            }
        }

        @Override
        public int hashCode() {
            LOG.warn("I was called : hashCode");
            // We currently just care about the row key since we're within the same table
            return Arrays.hashCode(startPartitionKey);
        }

        @Override
        public boolean equals(Object o) {
            LOG.warn("I was called : equals");
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TableSplit that = (TableSplit) o;

            return this.compareTo(that) == 0;
        }

        @Override
        public String toString() {
            LOG.warn("I was called : toString");
            return Objects.toStringHelper(this)
                    .add("startPartitionKey", Bytes.pretty(startPartitionKey))
                    .add("endPartitionKey", Bytes.pretty(endPartitionKey))
                    .add("locations", Arrays.toString(locations))
                    .toString();
        }
    }

    class KuduTableRecordReader implements RecordReader<NullWritable, HiveKuduWritable> {

        private final NullWritable currentKey = NullWritable.get();
        private RowResult currentValue;
        private RowResultIterator iterator;
        private KuduScanner scanner;
        private KuduTableSplit tableSplit;
        private Type[] types;
        private KuduClient client;
        private boolean first = true;
        private long rowCount;

        public KuduTableRecordReader(KuduTableSplit tableSplit, KuduClient client) throws IOException, InterruptedException {
            LOG.warn("I was called : TableRecordReader");

            this.tableSplit = tableSplit;
            this.client = client;
            scanner = KuduScanToken.deserializeIntoScanner(tableSplit.getScanTokenSerialized(), client);
            
            Schema schema = table.getSchema();
            types = new Type[schema.getColumnCount()];
            for (int i = 0; i < types.length; i++) {
                types[i] = schema.getColumnByIndex(i).getType();
                LOG.warn("Setting types array "+ i + " to " + types[i].name());
            }
            // Calling this now to set iterator.
            tryRefreshIterator();
            rowCount = 0L;
        }

        @Override
        public boolean next(NullWritable o, HiveKuduWritable o2) throws IOException {
            LOG.warn("I was called : next");
            
            // TODO read from empty kudu table
            
            if (!iterator.hasNext()) {
                tryRefreshIterator();
                if (!iterator.hasNext()) {
                    // Means we still have the same iterator, we're done
                    return false;
                }
            }
            
            currentValue = iterator.next();
            o = currentKey;
            o2.clear();
            for (int i = 0; i < types.length; i++) {
                switch(types[i]) {
                    case STRING: {
                        o2.set(i, currentValue.getString(i));
                        break;
                    }
                    case FLOAT: {
                        o2.set(i, currentValue.getFloat(i));
                        break;
                    }
                    case DOUBLE: {
                        o2.set(i, currentValue.getDouble(i));
                        break;
                    }
                    case BOOL: {
                        o2.set(i, currentValue.getBoolean(i));
                        break;
                    }
                    case INT8: {
                        o2.set(i, currentValue.getByte(i));
                        break;
                    }
                    case INT16: {
                        o2.set(i, currentValue.getShort(i));
                        break;
                    }
                    case INT32: {
                        o2.set(i, currentValue.getInt(i));
                        break;
                    }
                    case INT64: {
                        o2.set(i, currentValue.getLong(i));
                        break;
                    }
                    case UNIXTIME_MICROS: {
                    	long epoch_micros_seconds = currentValue.getLong(i);
                    	long epoch_seconds = epoch_micros_seconds/1000000L;
                    	long nano_seconds_adjustment = (epoch_micros_seconds%1000000L) * 1000L;
                    	Instant instant = Instant.ofEpochSecond(epoch_seconds, nano_seconds_adjustment);
                        o2.set(i, Timestamp.from(instant));
                        break;
                    }
                    case BINARY: {
                        o2.set(i, currentValue.getBinaryCopy(i));
                        break;
                    }
                    default:
                        throw new IOException("Cannot write Object '"
                                + currentValue.getColumnType(i).name() + "' as type: " + types[i].name());
                }
                LOG.warn("Value returned " + o2.get(i));
        	}
            
            this.rowCount += 1L;
            return true;
        }

        @Override
        public NullWritable createKey() {
            LOG.warn("I was called : createKey");
            return NullWritable.get();
        }

        @Override
        public HiveKuduWritable createValue() {
            LOG.warn("I was called : createValue");
            return new HiveKuduWritable(types);
        }

        @Override
        public long getPos() throws IOException {
            LOG.warn("I was called : getPos");
            return this.rowCount;
            //TODO: Get progress
        }

        /**
         * If the scanner has more rows, get a new iterator else don't do anything.
         * @throws IOException
         */
        private void tryRefreshIterator() throws IOException {
            LOG.warn("I was called : tryRefreshIterator");
            if (!scanner.hasMoreRows()) {
            	this.close();
                return;
            }
            try {
                iterator = scanner.nextRows();
            } catch (Exception e) {
                throw new IOException("Couldn't get scan data", e);
            }
        }

        /*
        Mapreduce code for reference

        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return currentKey;
        }

        @Override
        public RowResult getCurrentValue() throws IOException, InterruptedException {
            return currentValue;
        }
        */

        @Override
        public float getProgress() throws IOException {
            LOG.warn("I was called : getProgress");
            // TODO Guesstimate progress
            return 0;
        }


        @Override
        public void close() throws IOException {
            LOG.warn("I was called : close");
            try {
                scanner.close();
            } catch (NullPointerException npe) {
                LOG.warn("The scanner is supposed to be open but its not. TODO: Fix me.");
            }
            catch (Exception e) {
                throw new IOException(e);
            }
//            this.client.close();
        }
    }
}