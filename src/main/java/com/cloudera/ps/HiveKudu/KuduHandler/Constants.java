package com.cloudera.ps.HiveKudu.KuduHandler;

/**
 * Created by bimal on 4/11/16.
 */
import org.apache.hadoop.conf.Configuration;

public final class Constants {

    public static final String LIST_COLUMNS = "columns";
    public static final String LIST_COLUMN_TYPES = "columns.types";
    public static final String INPUT_FETCH_SIZE = "kudu.storage.handler.input.fetch.size";

    public static final int DEFAULT_INPUT_FETCH_SIZE = 1000;
    private Constants() {
    }

    public static int getInputFetchSize(Configuration conf) {
        return conf.getInt(INPUT_FETCH_SIZE, 10000);
    }

}