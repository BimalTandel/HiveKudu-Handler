package org.apache.hadoop.hive.kududb.KuduHandler;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;
import org.kududb.mapreduce.KuduTableOutputFormat;

import java.io.IOException;

/**
 * Created by bimal on 4/13/16.
 */
public class HiveKuduTableOutputFormat implements OutputFormat {

    private  KuduTableOutputFormat kuduOutputFormat;
    public HiveKuduTableOutputFormat() {
        kuduOutputFormat = new KuduTableOutputFormat();
    }

    @Override
    public RecordWriter getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
        TaskAttemptContext taskContext = (TaskAttemptContext) jobConf;
        try {
            return (org.apache.hadoop.mapred.RecordWriter) kuduOutputFormat.getRecordWriter(taskContext);
        } catch (InterruptedException ioe) {
            throw new IOException(StringUtils.stringifyException(ioe));
        }
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {

        JobContext jobContext = (JobContext) jobConf;
        try {
            kuduOutputFormat.checkOutputSpecs(jobContext);
        } catch (InterruptedException ioe){
            throw new IOException(StringUtils.stringifyException(ioe));
        }
    }

}
