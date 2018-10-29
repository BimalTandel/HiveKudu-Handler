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

/**
 * Created by bimal on 4/11/16.
 */

public final class HiveKuduConstants {

    //Table Properties
    public static final String LIST_COLUMNS = "columns";
    public static final String LIST_COLUMN_TYPES = "columns.types";
    public static final String MASTER_ADDRESS_NAME = "kudu.master_addresses";
    public static final String TABLE_NAME = "kudu.table_name";
    public static final String KEY_COLUMNS = "kudu.key_columns";
    public static final String PARTITION_TYPE = "kudu.partition_type";
    public static final String NUM_PARTITION = "kudu.num_partition";
    public static final String PARTITION_COLUMNS = "kudu.partition_columns";
    public static final String REPLICATION_FACTOR = "kudu.replication_factor";

    //SerDe Properties

    //MapReduce Properties
    public static final String MR_INPUT_TABLE_NAME = "kudu.mapreduce.input.table";
    public static final String MR_OUTPUT_TABLE_NAME = "kudu.mapreduce.output.table";
    public static final String MR_MASTER_ADDRESS_NAME = "kudu.mapreduce.master.addresses";
    public static final String MR_PROPERTY_PREFIX = "kudu.mapreduce.";
    //DEFAULT VALUES & Getters for Default values


    private HiveKuduConstants() {
    }
}