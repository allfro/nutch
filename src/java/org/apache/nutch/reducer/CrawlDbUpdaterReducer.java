package org.apache.nutch.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;

import java.io.IOException;

public class CrawlDbUpdaterReducer extends
        Reducer<Text, CrawlDatum, Text, CrawlDatum> {

    long generateTime;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        generateTime = context.getConfiguration().getLong(Nutch.GENERATE_TIME_KEY, 0L);
    }

    @Override
    protected void reduce(Text key, Iterable<CrawlDatum> values, Context context)
            throws IOException, InterruptedException {
        CrawlDatum orig = new CrawlDatum();
        LongWritable genTime = new LongWritable(0L);
        genTime.set(0L);
        for (CrawlDatum val : values) {
            if (val.getMetaData().containsKey(Nutch.WRITABLE_GENERATE_TIME_KEY)) {
                LongWritable gt = (LongWritable) val.getMetaData().get(
                        Nutch.WRITABLE_GENERATE_TIME_KEY);
                genTime.set(gt.get());
                if (genTime.get() != generateTime) {
                    orig.set(val);
                    genTime.set(0L);
                }
            } else {
                orig.set(val);
            }
        }
        if (genTime.get() != 0L) {
            orig.getMetaData().put(Nutch.WRITABLE_GENERATE_TIME_KEY, genTime);
        }
        context.write(key, orig);
    }
}
