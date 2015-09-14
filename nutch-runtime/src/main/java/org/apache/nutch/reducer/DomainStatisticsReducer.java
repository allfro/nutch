package org.apache.nutch.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class DomainStatisticsReducer extends
        Reducer<Text, LongWritable, LongWritable, Text> {
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long total = 0;

        for (LongWritable val : values) {
            total += val.get();
        }

        context.write(new LongWritable(total), key);
    }
}
