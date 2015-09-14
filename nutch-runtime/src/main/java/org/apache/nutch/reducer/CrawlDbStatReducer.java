package org.apache.nutch.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class CrawlDbStatReducer extends
        Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        String k = key.toString();
        if (k.equals("T")) {
            // sum all values for this key
            long sum = 0;
            for (LongWritable value: values) {
                sum += value.get();
            }
            // output sum
            context.write(key, new LongWritable(sum));
        } else if (k.startsWith("status") || k.startsWith("retry")) {
            LongWritable cnt = new LongWritable();
            for (LongWritable value: values) {
                cnt.set(cnt.get() + value.get());
            }
            context.write(key, cnt);
        } else if (k.equals("scx")) {
            LongWritable cnt = new LongWritable(Long.MIN_VALUE);
            for (LongWritable value: values) {
                if (cnt.get() < value.get())
                    cnt.set(value.get());
            }
            context.write(key, cnt);
        } else if (k.equals("scn")) {
            LongWritable cnt = new LongWritable(Long.MAX_VALUE);
            for (LongWritable value: values) {
                if (cnt.get() > value.get())
                    cnt.set(value.get());
            }
            context.write(key, cnt);
        } else if (k.equals("sct")) {
            LongWritable cnt = new LongWritable();
            for (LongWritable value: values) {
                cnt.set(cnt.get() + value.get());
            }
            context.write(key, cnt);
        }
    }
}
