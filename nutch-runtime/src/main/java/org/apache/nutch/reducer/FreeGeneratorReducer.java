package org.apache.nutch.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.io.SelectorEntryWritable;

import java.io.IOException;
import java.util.HashMap;

public class FreeGeneratorReducer extends
        Reducer<Text, SelectorEntryWritable, Text, CrawlDatum> {

    @Override
    protected void reduce(Text key, Iterable<SelectorEntryWritable> values, Context context) throws IOException, InterruptedException {
        // pick unique urls from values - discard the reduce key due to hash
        // collisions
        HashMap<Text, Integer> unique = new HashMap<>();
        for (SelectorEntryWritable entry: values) {
            if (unique.containsKey(entry.url))
                continue;
            unique.put(entry.url, 1);
            context.write(entry.url, entry.datum);
        }
    }
}
