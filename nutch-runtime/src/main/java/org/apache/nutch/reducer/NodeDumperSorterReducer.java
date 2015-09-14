package org.apache.nutch.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class NodeDumperSorterReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {


    private Configuration conf;
    private long topN;

    @Override
    /**
     * Configures the job, sets the flag for type of content and the topN number
     * if any.
     */
    public void setup(Context context) {
        this.conf = context.getConfiguration();
        this.topN = conf.getLong("topn", Long.MAX_VALUE);
    }


    @Override
    /**
     * Flips and collects the url and numeric sort value.
     */
    public void reduce(FloatWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // take the negative of the negative to get original value, sometimes 0
        // value are a little weird
        float val = key.get();
        FloatWritable number = new FloatWritable(val == 0 ? 0 : -val);
        long numCollected = 0;

        // collect all values, this time with the url as key
        for (Text value: values) {
            if (numCollected == topN)
                break;
            Text url = WritableUtils.clone(value, conf);
            context.write(url, number);
            numCollected++;
        }
    }
}
