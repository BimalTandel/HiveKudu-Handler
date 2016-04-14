package com.cloudera.ps.HiveKudu.KuduHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.client.KuduClient;
import org.kududb.client.CreateTableOptions;
import org.kududb.mapred.KuduTableInputFormat;
import org.kududb.mapred.KuduTableOutputFormat;

import java.util.*;

/**
 * Created by bimal on 4/11/16.
 */
public class KuduStorageHandler extends DefaultStorageHandler
        implements HiveMetaHook, HiveStoragePredicateHandler {

    private static final Log LOG = LogFactory.getLog(KuduStorageHandler.class);

    private Configuration conf;

    private String kuduMaster;
    private KuduClient client;

    private KuduClient getKuduClient() throws MetaException {
        LOG.warn("I was called : getKuduClient");
        try {
            if (client == null) {
                client = new KuduClient.KuduClientBuilder(kuduMaster).build();
            }
            return client;
        } catch (Exception ioe){
            throw new MetaException(StringUtils.stringifyException(ioe));
        }
    }

    public KuduStorageHandler() {
    }

    @Override
    public Configuration getConf() {
        LOG.warn("I was called : getConf");
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        LOG.warn("I was called : setConf");
        this.conf = conf;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        LOG.warn("I was called : getMetaHook");
        return this;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc,
                                            Map<String, String> jobProperties) {
        LOG.warn("I was called : configureInputJobProperties");
        configureJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc,
                                             Map<String, String> jobProperties) {
        LOG.warn("I was called : configureOutputJobProperties");
        configureJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc,
                                            Map<String, String> jobProperties) {
        LOG.warn("I was called : configureTableJobProperties");
        configureJobProperties(tableDesc, jobProperties);
    }

    private void configureJobProperties(TableDesc tableDesc,
                                        Map<String, String> jobProperties) {
        LOG.warn("I was called : configureJobProperties");
        if (LOG.isDebugEnabled()) {
            LOG.debug("tabelDesc: " + tableDesc);
            LOG.debug("jobProperties: " + jobProperties);
        }
        /*
        TODO: Implement reading provided properties and load jobProperties
         */


        String tblName = tableDesc.getTableName();
        Properties tblProps = tableDesc.getProperties();
        String columnNames = tblProps.getProperty(Constants.LIST_COLUMNS);

        LOG.warn("kudu.mapreduce.master.addresses" + tblProps.getProperty("kudu.master_addresses"));

        jobProperties.put("kudu.mapreduce.output.table", tblProps.getProperty("kudu.table_name"));
        jobProperties.put("kudu.mapreduce.master.addresses", tblProps.getProperty("kudu.master_addresses"));
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider()
            throws HiveException {
        LOG.warn("I was called : getAuthorizationProvider");
        return new DefaultHiveAuthorizationProvider();
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return KuduTableInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return KuduTableOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return KuduSerDe.class;
    }

    @Override
    public DecomposedPredicate decomposePredicate(JobConf jobConf,
                                                  Deserializer deserializer, ExprNodeDesc predicate) {
        LOG.warn("I was called : decomposePredicate");
        // No Idea how to implement Predicate push down. Need to read more about this and understand how it will work in Kudu.
        DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
        return decomposedPredicate;
    }

    private String getKuduTableName(Table tbl) {

        LOG.warn("I was called : getKuduTableName");

        String tableName = tbl.getParameters().get("kudu.table_name");
        if (tableName == null) {
            tableName = tbl.getTableName();
        }
        return tableName;
    }

    private void printSchema(Schema schema) {

        LOG.warn("I was called : printSchema");

        if (schema == null) {
              return;
            }

        LOG.debug("Printing schema for Kudu table..");
        for (ColumnSchema sch : schema.getColumns()) {
            LOG.debug("Column Name: " + sch.getName()
                    + " [" + sch.getType().getName() + "]"
                    + " key column: [" + sch.isKey() + "]"
              );
        }
    }


    @Override
    public void preCreateTable(Table tbl) throws MetaException {

        LOG.warn("I was called : preCreateTable");


        boolean isExternal = MetaStoreUtils.isExternalTable(tbl);

        if (tbl.getSd().getLocation() != null) {
            throw new MetaException("LOCATION may not be specified for Kudu");
        }

        kuduMaster = tbl.getParameters().get("kudu.master_addresses");
        LOG.debug("Kudu Master is" + kuduMaster);

        // TODO Auto-generated method stub
        String tablename = getKuduTableName(tbl);

        LOG.debug("Tablename is " + tablename);

        LOG.warn(tbl.getSd().getSerdeInfo().toString());

        try {
            List<String> keyColumns = Arrays.asList(tbl.getParameters().get("kudu.key_columns").split("\\s*,\\s*"));

            List<FieldSchema> tabColumns = tbl.getSd().getCols();

            int numberOfCols = tabColumns.size();
            List<ColumnSchema> columns = new ArrayList<>(numberOfCols);

            for (FieldSchema fields : tabColumns) {

                ColumnSchema columnSchema = new ColumnSchema
                        .ColumnSchemaBuilder(fields.getName(), HiveKuduBridgeUtils.hiveTypeToKuduType(fields.getType()))
                        .key(keyColumns.contains(fields.getName()))
                        .nullable(!keyColumns.contains(fields.getName()))
                        .build();

                columns.add(columnSchema);
            }

            Schema schema = new Schema(columns);

            if (null != schema) {
                printSchema(schema);
            }

            CreateTableOptions createTableOptions = new CreateTableOptions();

            //add support for partition and buckets
            getKuduClient().createTable(tablename, schema, createTableOptions);
        } catch (Exception se) {
            LOG.error("Error creating Kudu table: " + tablename);
            throw new MetaException(StringUtils.stringifyException(se));
        } finally {
            try {
                getKuduClient().shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void commitCreateTable(Table tbl) throws MetaException {
        LOG.warn("I was called : commitCreateTable");
        // TODO Auto-generated method stub
    }

    @Override
    public void preDropTable(Table tbl) throws MetaException {
        LOG.warn("I was called : preDropTable");
        // nothing to do
    }

    @Override
    public void commitDropTable(Table tbl, boolean deleteData)
            throws MetaException {
        LOG.warn("I was called : commitDropTable");
        // TODO Auto-generated method stub
    }

    @Override
    public void rollbackCreateTable(Table tbl) throws MetaException {
        LOG.warn("I was called : rollbackCreateTable");
        // TODO Auto-generated method stub
    }

    @Override
    public void rollbackDropTable(Table tbl) throws MetaException {
        LOG.warn("I was called : rollbackDropTable");
        // TODO Auto-generated method stub
    }

}
