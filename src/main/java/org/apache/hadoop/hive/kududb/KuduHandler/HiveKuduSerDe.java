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
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
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

public class HiveKuduSerDe implements SerDe {

    private static final Log LOG = LogFactory.getLog(HiveKuduSerDe.class);

    private StructObjectInspector objectInspector;
    private List<Object> deserializeCache;

    private int fieldCount;

    private Type[] types;

    public HiveKuduSerDe() {
    }

    @Override
    public void initialize(Configuration sysConf, Properties tblProps)
        throws SerDeException {
        LOG.debug("tblProps: " + tblProps);

        String columnNameProperty = tblProps
                .getProperty(HiveKuduConstants.LIST_COLUMNS);
        String columnTypeProperty = tblProps
                .getProperty(HiveKuduConstants.LIST_COLUMN_TYPES);

        LOG.info("Column Names: " + columnNameProperty);
        LOG.info("Column Types: " + columnTypeProperty);

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

        types = new Type[columnTypes.length];

        for (int i = 0; i < types.length; i++) {
            types[i] = HiveKuduBridgeUtils.hiveTypeToKuduType(columnTypes[i]);
        }

        this.fieldCount = types.length;

        final List<ObjectInspector> fieldOIs = new ArrayList<>(columnTypes.length);

        for (int i = 0; i < types.length; i++) {
            ObjectInspector oi = HiveKuduBridgeUtils.getObjectInspector(types[i], columnTypes[i]);
            fieldOIs.add(oi);
        }

        this.objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, fieldOIs);
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return HiveKuduSerdeRow.class;
    }

    @Override
    public HiveKuduSerdeRow serialize(Object row, ObjectInspector inspector) {
        return new HiveKuduSerdeRow(row, inspector, types);
    }

    @Override
    public Object deserialize(Writable record) throws SerDeException {
        if (!(record instanceof HiveKuduStruct)) {
            throw new SerDeException("Expected HiveKuduStruct, received "
                    + record.getClass().getName());
        }
        HiveKuduStruct tuple = (HiveKuduStruct) record;
        deserializeCache.clear();
        for (int i = 0; i < fieldCount; i++) {
            Object o = tuple.get(i);
            deserializeCache.add(o);
        }
        return deserializeCache;
    }

    @Override
    public SerDeStats getSerDeStats() {
        // TODO How to implement this?
        return null;
    }
}


