package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Runs a single link analysis iteration.
 */
public class AnalyzerMapper extends
        Mapper<Text, Writable, Text, ObjectWritable> {

    private Configuration conf;

    /**
     * Configures the job, sets the damping factor, rank one score, and other
     * needed values for analysis.
     */
    public void setup(Context context) {
        this.conf = context.getConfiguration();
    }

    @Override
    /**
     * Convert values to ObjectWritable
     */
    protected void map(Text key, Writable value, Context context) throws IOException, InterruptedException {
        ObjectWritable objWrite = new ObjectWritable();
        objWrite.set(WritableUtils.clone(value, conf));
        context.write(key, objWrite);
    }


}
