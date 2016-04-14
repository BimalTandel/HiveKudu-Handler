package com.cloudera.ps.HiveKudu.KuduHandler;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.kududb.Type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;


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
                return Type.INT8;
            case "smallint":
                return Type.INT16;
            case "int":
                return Type.INT32;
            case "bigint":
                return Type.INT64;
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
                throw new SerDeException("Unrecognized column type: " + hiveType + " not supported in Kudu");
        }
    }

    public static ObjectInspector getObjectInspector(Type kuduType,
                                                     String hiveType) throws SerDeException {
        switch (kuduType) {
            case STRING:
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            case FLOAT:
                return PrimitiveObjectInspectorFactory.javaFloatObjectInspector;
            case DOUBLE:
                return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
            case BOOL:
                return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
            case INT8:
                return PrimitiveObjectInspectorFactory.javaByteObjectInspector;
            case INT16:
                return PrimitiveObjectInspectorFactory.javaShortObjectInspector;
            case INT32:
                return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
            case INT64:
                return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
            case TIMESTAMP:
                return PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;
            case BINARY:
                return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
            default:
                throw new SerDeException("Cannot find getObjectInspector for: "
                        + hiveType);
        }
    }

    public static Object deparseObject(Object field, ObjectInspector fieldOI)
            throws SerDeException {
        switch (fieldOI.getCategory()) {
            case PRIMITIVE: {
                PrimitiveObjectInspector oi = (PrimitiveObjectInspector) fieldOI;
                return oi.getPrimitiveJavaObject(field);
            }

            //Kudu doesnt support LIST or MAP based data types

            default:
                throw new SerDeException("Unexpected fieldOI: " + fieldOI);
        }
    }


    public static Object readObject(DataInput in, Type kuduType)
            throws IOException {
        switch (kuduType) {
            case STRING:
                return in.readUTF();
            case FLOAT:
                return Float.valueOf(in.readFloat());
            case DOUBLE:
                return Double.valueOf(in.readDouble());
            case BOOL:
                return Boolean.valueOf(in.readBoolean());
            case INT8:
                return Byte.valueOf(in.readByte());
            case INT16:
                return Short.valueOf(in.readShort());
            case INT32:
                return Integer.valueOf(in.readInt());
            case INT64:
                return Long.valueOf(in.readLong());
            case TIMESTAMP: {
                long time = in.readLong();
                return new Timestamp(time);
            }
            case BINARY: {
                int size = in.readInt();
                byte[] b = new byte[size];
                in.readFully(b);
                return b;
            }
            default:
                throw new IOException("Cannot read Object for type: " + kuduType.name());
        }
    }

    public static void writeObject(Object obj, Type kuduType, DataOutput out)
            throws IOException {
        switch (kuduType) {
            case STRING: {
                String s = obj.toString();
                out.writeUTF(s);
                return;
            }
            case FLOAT: {
                Float f = (Float) obj;
                out.writeFloat(f);
                return;
            }
            case DOUBLE: {
                Double d = (Double) obj;
                out.writeDouble(d);
                return;
            }
            case BOOL: {
                Boolean b = (Boolean) obj;
                out.writeBoolean(b);
                return;
            }
            case INT8: {
                Byte b = (Byte) obj;
                out.writeByte(b.intValue());
                return;
            }
            case INT16: {
                Short s = (Short) obj;
                out.writeShort(s.shortValue());
                return;
            }
            case INT32: {
                Integer i = (Integer) obj;
                out.writeInt(i.intValue());
                return;
            }
            case INT64: {
                Long l = (Long) obj;
                out.writeLong(l.longValue());
                return;
            }
            case TIMESTAMP: {
                Timestamp time = (Timestamp) obj;
                out.writeLong(time.getTime());
                return;
            }
            case BINARY: {
                byte[] b = (byte[]) obj;
                out.writeInt(b.length);
                out.write(b);
                return;
            }
            default:
                throw new IOException("Cannot write Object '"
                        + obj.getClass().getSimpleName() + "' as type: " + kuduType.name());
        }
    }
}
