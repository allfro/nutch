package org.apache.nutch.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.SelectorEntryWritable;

import java.io.IOException;

public class SelectorInverseMapper extends
        Mapper<FloatWritable, SelectorEntryWritable, Text, SelectorEntryWritable> {
    @Override
    protected void map(FloatWritable key, SelectorEntryWritable entry, Context context)
            throws IOException, InterruptedException {
        context.write(entry.url, entry);
    }
}
