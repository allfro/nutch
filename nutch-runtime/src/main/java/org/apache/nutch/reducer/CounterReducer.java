package org.apache.nutch.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.scoring.webgraph.LinkRank;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class CounterReducer extends
        Reducer<Text, LongWritable, Text, LongWritable> {
    private static Text numNodes = new Text(LinkRank.NUM_NODES);

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long total = 0;
        for (LongWritable value: values) {
            total += value.get();
        }
        context.write(numNodes, new LongWritable(total));

    }
}
