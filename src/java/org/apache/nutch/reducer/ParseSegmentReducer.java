package org.apache.nutch.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ndouba on 8/29/15.
 */
public class ParseSegmentReducer extends Reducer<Text, Writable, Text, Writable> {

    public static final Logger LOG = LoggerFactory.getLogger(ParseSegmentReducer.class);

    @Override
    protected void reduce(Text key, Iterable<Writable> values, Context context) throws IOException, InterruptedException {
        context.write(key, values.iterator().next()); // collect first value
    }

}
