package org.apache.nutch.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.CrawlDatum;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class CrawlDbTopNMapper extends
        Mapper<Text, CrawlDatum, FloatWritable, Text> {
    private static final FloatWritable fw = new FloatWritable();
    private float min = 0.0f;

    @Override
    public void setup(Context context) {
        min = context.getConfiguration().getFloat("db.reader.topn.min", 0.0f);
    }

    @Override
    protected void map(Text key, CrawlDatum value, Context context) throws IOException, InterruptedException {
        if (value.getScore() < min)
            return; // don't collect low-scoring records
        fw.set(-value.getScore()); // reverse sorting order
        context.write(fw, key); // invert mapping: score -> url
    }

}
