/**
 * Copyright 2016 Bimal Tandel

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.hadoop.hive.kududb.KuduHandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.CreateTableOptions;
import org.kududb.mapred.HiveKuduTableInputFormat;
import org.kududb.mapred.HiveKuduTableOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created by bimal on 4/11/16.
 */

@SuppressWarnings({ "deprecation", "rawtypes" })
public class KuduStorageHandler extends DefaultStorageHandler
        implements HiveMetaHook, HiveStoragePredicateHandler {

    private static final Log LOG = LogFactory.getLog(KuduStorageHandler.class);

    private Configuration conf;

    private String kuduMaster;
    private String kuduTableName;

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return HiveKuduTableInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return HiveKuduTableOutputFormat.class;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return HiveKuduSerDe.class;
    }

    private KuduClient getKuduClient(String master) throws MetaException {
        try {

            return new KuduClient.KuduClientBuilder(master).build();
        } catch (Exception ioe){
            throw new MetaException("Error creating Kudu Client: " + ioe);
        }
    }

    public KuduStorageHandler() {
        // TODO: Empty initializer??
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return this;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc,
                                            Map<String, String> jobProperties) {
        configureJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc,
                                             Map<String, String> jobProperties) {
        configureJobProperties(tableDesc, jobProperties);
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc,
                                            Map<String, String> jobProperties) {
        configureJobProperties(tableDesc, jobProperties);
    }

    private void configureJobProperties(TableDesc tableDesc,
                                        Map<String, String> jobProperties) {

        //This will always have the DB Name qualifier of Hive. Dont use this to set Kudu Tablename.
        String tblName = tableDesc.getTableName();
        LOG.debug("Hive Table Name:" + tblName);
        Properties tblProps = tableDesc.getProperties();
        String columnNames = tblProps.getProperty(HiveKuduConstants.LIST_COLUMNS);
        String columnTypes = tblProps.getProperty(HiveKuduConstants.LIST_COLUMN_TYPES);
        LOG.debug("Columns names:" + columnNames);
        LOG.debug("Column types:" + columnTypes);

        if (columnNames.length() == 0) {
            //TODO: Place keeper to insert SerDeHelper code to connect to Kudu to extract column names.
            LOG.warn("SerDe currently doesn't support column names and types. Please provide it explicitly");
        }

        //set map reduce properties.
        jobProperties.put(HiveKuduConstants.MR_INPUT_TABLE_NAME,
                tblProps.getProperty(HiveKuduConstants.TABLE_NAME));
        jobProperties.put(HiveKuduConstants.MR_OUTPUT_TABLE_NAME,
                tblProps.getProperty(HiveKuduConstants.TABLE_NAME));
        jobProperties.put(HiveKuduConstants.MR_MASTER_ADDRESS_NAME,
                tblProps.getProperty(HiveKuduConstants.MASTER_ADDRESS_NAME));

        LOG.debug("Kudu Table Name: " + tblProps.getProperty(HiveKuduConstants.TABLE_NAME));
        LOG.debug("Kudu Master Addresses: " + tblProps.getProperty(HiveKuduConstants.MASTER_ADDRESS_NAME));


        //set configuration property
        conf.set(HiveKuduConstants.MR_INPUT_TABLE_NAME,
                tblProps.getProperty(HiveKuduConstants.TABLE_NAME));
        conf.set(HiveKuduConstants.MR_OUTPUT_TABLE_NAME,
                tblProps.getProperty(HiveKuduConstants.TABLE_NAME));
        conf.set(HiveKuduConstants.MR_MASTER_ADDRESS_NAME,
                tblProps.getProperty(HiveKuduConstants.MASTER_ADDRESS_NAME));

        conf.set(HiveKuduConstants.TABLE_NAME,
                tblProps.getProperty(HiveKuduConstants.TABLE_NAME));
        conf.set(HiveKuduConstants.MASTER_ADDRESS_NAME,
                tblProps.getProperty(HiveKuduConstants.MASTER_ADDRESS_NAME));

        //set class variables
        kuduMaster = conf.get(HiveKuduConstants.MASTER_ADDRESS_NAME);
        kuduTableName = conf.get(HiveKuduConstants.TABLE_NAME);

        for (String key : tblProps.stringPropertyNames()) {
            if (key.startsWith(HiveKuduConstants.MR_PROPERTY_PREFIX)) {
                String value = tblProps.getProperty(key);
                jobProperties.put(key, value);
                //Also set configuration for Non Map Reduce Hive calls to the Handler
                conf.set(key, value);
            }
        }
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider()
            throws HiveException {
        return new DefaultHiveAuthorizationProvider();
    }

    @Override
    public DecomposedPredicate decomposePredicate(JobConf jobConf,
                                                  Deserializer deserializer, ExprNodeDesc predicate) {
        // TODO: Implement push down to Kudu here.
        DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
        return decomposedPredicate;
    }

    private String getKuduTableName(Table tbl) {

        String tableName = tbl.getParameters().getOrDefault(HiveKuduConstants.TABLE_NAME,  
        		conf.get(HiveKuduConstants.TABLE_NAME));
        if (tableName == null) {
            LOG.warn("Kudu Table name was not provided in table properties.");
            LOG.warn("Attempting to use Hive Table name");
            tableName = tbl.getTableName().replaceAll(".*\\.", "");
            LOG.warn("Kudu Table name will be: " + tableName);

        }
        return tableName;
    }

    private void printSchema(Schema schema) {
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
    public void preCreateTable(Table tbl)
            throws MetaException {
        KuduClient client = getKuduClient(tbl.getParameters().get(HiveKuduConstants.MASTER_ADDRESS_NAME));

        String tablename = getKuduTableName(tbl);
        boolean isExternal = MetaStoreUtils.isExternalTable(tbl);

        if (isExternal) {
        	try {
	            //TODO: Check if Kudu table exists to allow external table.
	            //TODO: Check if column and types are compatible with existing Kudu table.
	            KuduTable kuduTable = client.openTable(tablename);
	            List<ColumnSchema> kuduColumns = kuduTable.getSchema().getColumns();
	            StorageDescriptor sd = tbl.getSd();
	            List<FieldSchema> hiveCols = new ArrayList<FieldSchema>(kuduColumns.size());
	            for(ColumnSchema kuduCol:kuduColumns) {
	            	FieldSchema hiveFieldSchema = new FieldSchema(kuduCol.getName(), 
	            			HiveKuduBridgeUtils.kuduTypeToHiveType(kuduCol.getType()), 
	            			null);
	            	hiveCols.add(hiveFieldSchema);
	            }
	            sd.setCols(hiveCols);
	            return;
        	}
        	catch(SerDeException e) {
        		throw new MetaException("unable to convet kudu schema to hive schema "+e.getMessage());
        	}
        	catch(KuduException e) {
        		throw new MetaException("unable to open Kudu table "+tablename+". "+ e.getMessage());
        	}
        }
        
        
        // For internal tables
        if (tbl.getSd().getLocation() != null) {
            throw new MetaException("LOCATION may not be specified for Kudu");
        }

        
        try {
            List<String> keyColumns = Arrays.asList(tbl.getParameters().get(HiveKuduConstants.KEY_COLUMNS).split("\\s*,\\s*"));
            String partitionType = tbl.getParameters().get(HiveKuduConstants.PARTITION_TYPE);
            if(!partitionType.equals("HASH")) { // Currently only hash partition is supported
            	throw new MetaException("unsupported partition type "+ partitionType);
            }
            int numPartitions = Integer.parseInt(tbl.getParameters().get(HiveKuduConstants.NUM_PARTITION));
            List<String> partitionColumns = Arrays.asList(tbl.getParameters().get(HiveKuduConstants.PARTITION_COLUMNS).split("\\s*,\\s*"));
            int replicationFactor = Integer.parseInt(tbl.getParameters().getOrDefault(HiveKuduConstants.REPLICATION_FACTOR, "3"));

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

            printSchema(schema);

            CreateTableOptions createTableOptions = new CreateTableOptions();
            if(partitionType.toUpperCase().equals("HASH")) {  // Only hash partition is supported for now
            	createTableOptions.addHashPartitions(partitionColumns, numPartitions);
            }
            createTableOptions.setNumReplicas(replicationFactor);

            client.createTable(tablename, schema, createTableOptions);

        } catch (Exception se) {
            throw new MetaException("Error creating Kudu table: " + tablename + ":" + se);
        } finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void commitCreateTable(Table tbl) throws MetaException {
        // Nothing to do
    }

    @Override
    public void preDropTable(Table tbl) throws MetaException {
        // Nothing to do

    }

    @Override
    public void commitDropTable(Table tbl, boolean deleteData)
            throws MetaException {
        KuduClient client = getKuduClient(tbl.getParameters().get(HiveKuduConstants.MASTER_ADDRESS_NAME));
        String tablename = getKuduTableName(tbl);
        boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
        try {
            if (deleteData && !isExternal) {
                client.deleteTable(tablename);
            }
        } catch (Exception ioe) {
            throw new MetaException("Error dropping table:" +tablename);
        } finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void rollbackCreateTable(Table tbl) throws MetaException {
        KuduClient client = getKuduClient(tbl.getParameters().get(HiveKuduConstants.MASTER_ADDRESS_NAME));
        String tablename = getKuduTableName(tbl);
        boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
        try {
            if ( client.tableExists(tablename) && !isExternal) {
                client.deleteTable(tablename);
            }
        } catch (Exception ioe) {
            throw new MetaException("Error dropping table while rollback of create table:" +tablename);
        } finally {
            try {
                client.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void rollbackDropTable(Table tbl) throws MetaException {
        // Nothing to do
    }

}
