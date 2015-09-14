package org.apache.nutch.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class CrawlDbTopNReducer extends
        Reducer<FloatWritable, Text, FloatWritable, Text> {
    private long topN;
    private long count = 0L;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        topN = configuration.getLong("db.reader.topn", 100) / context.getNumReduceTasks();
    }

    @Override
    protected void reduce(FloatWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value: values) {
            if (count == topN)
                break;
            key.set(-key.get());
            context.write(key, value);
            count++;
        }
    }
}
