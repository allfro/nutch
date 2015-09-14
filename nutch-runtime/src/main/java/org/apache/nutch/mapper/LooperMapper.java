package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.LinkDatum;

import java.io.IOException;

/**
 * Follows a route path looking for the start url of the route. If the start
 * url is found then the route is a cyclical path.
 */
public class LooperMapper extends Mapper<Text, Writable, Text, ObjectWritable> {

    private Configuration conf;

    /**
     * Configure the job.
     */
    public void setup(Context context) {
        this.conf = context.getConfiguration();
    }

    @Override
    protected void map(Text key, Writable value, Context context) throws IOException, InterruptedException {
        ObjectWritable objWrite = new ObjectWritable();
        Writable cloned = null;
        if (value instanceof LinkDatum) {
            cloned = new Text(((LinkDatum) value).getUrl());
        } else {
            cloned = WritableUtils.clone(value, conf);
        }
        objWrite.set(cloned);
        context.write(key, objWrite);
    }

}
