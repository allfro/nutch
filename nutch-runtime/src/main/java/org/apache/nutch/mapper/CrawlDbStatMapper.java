package org.apache.nutch.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.CrawlDatum;

import java.io.IOException;
import java.net.URL;

/**
 * Created by ndouba on 9/2/15.
 */
public class CrawlDbStatMapper extends
        Mapper<Text, CrawlDatum, Text, LongWritable> {
    LongWritable COUNT_1 = new LongWritable(1);
    private boolean sort = false;

    @Override
    protected void map(Text key, CrawlDatum value, Context context) throws IOException, InterruptedException {
        context.write(new Text("T"), COUNT_1);
        context.write(new Text("status " + value.getStatus()), COUNT_1);
        context.write(new Text("retry " + value.getRetriesSinceFetch()), COUNT_1);
        context.write(new Text("s"), new LongWritable((long) (value.getScore() * 1000.0)));
        if (sort) {
            URL u = new URL(key.toString());
            String host = u.getHost();
            context.write(new Text("status " + value.getStatus() + " " + host), COUNT_1);
        }
    }
}
