package com.cloudera.ps.HiveKudu.KuduHandler;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.kududb.Type;


/**
 * Created by bimal on 4/12/16.
 */
public class HiveKuduBridgeUtils {

    public static Type hiveTypeToKuduType(String hiveType) throws SerDeException {
        final String lchiveType = hiveType.toLowerCase();
        switch(lchiveType) {
            case "string":
            case "varchar":
            case "char":
                return Type.STRING;

            case "tinyint":
            case "smallint":
            case "int":
            case "bigint":
                return Type.INT8;

            case "float":
                return Type.FLOAT;

            case "double":
                return Type.DOUBLE;

            case "timestamp":
                return Type.TIMESTAMP;

            case "boolean":
                return Type.BOOL;

            case "binary":
                return Type.BINARY;
            default:
                throw new SerDeException("Unrecognized column type: " + hiveType);
        }
    }
}
