package org.apache.nutch.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.io.SelectorEntryWritable;

import java.io.IOException;

public class PartitionReducer extends
        Reducer<Text, SelectorEntryWritable, Text, CrawlDatum> {
    @Override
    protected void reduce(Text key, Iterable<SelectorEntryWritable> values, Context context)
            throws IOException, InterruptedException {
        // if using HashComparator, we get only one input key in case of
        // hash collision
        // so use only URLs from values
        for (SelectorEntryWritable entry : values) {
            context.write(entry.url, entry.datum);
        }
    }

}
