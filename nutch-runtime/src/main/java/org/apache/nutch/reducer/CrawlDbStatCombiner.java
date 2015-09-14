package org.apache.nutch.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class CrawlDbStatCombiner extends
        Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable val = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        val.set(0L);
        String k = key.toString();
        if (!k.equals("s")) {
            for (LongWritable cnt: values) {
                val.set(val.get() + cnt.get());
            }
            context.write(key, val);
        } else {
            long total = 0;
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (LongWritable cnt: values) {
                if (cnt.get() < min)
                    min = cnt.get();
                if (cnt.get() > max)
                    max = cnt.get();
                total += cnt.get();
            }
            context.write(new Text("scn"), new LongWritable(min));
            context.write(new Text("scx"), new LongWritable(max));
            context.write(new Text("sct"), new LongWritable(total));
        }
    }
}
