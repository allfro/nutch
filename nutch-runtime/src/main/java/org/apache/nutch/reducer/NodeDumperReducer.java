package org.apache.nutch.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class NodeDumperReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {


    private long topn = Long.MAX_VALUE;
    private boolean sum = false;

    @Override
    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
        this.topn = configuration.getLong("topn", Long.MAX_VALUE);
        this.sum = configuration.getBoolean("sum", false);
    }
    @Override
    /**
     * Outputs either the sum or the top value for this record.
     */
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        long numCollected = 0;
        float sumOrMax = 0;
        float val;

        // collect all values, this time with the url as key
        for (FloatWritable value: values) {
            if (numCollected == topn)
                break;
            val = value.get();

            if (sum) {
                sumOrMax += val;
            } else {
                if (sumOrMax < val) {
                    sumOrMax = val;
                }
            }

            numCollected++;
        }

        context.write(key, new FloatWritable(sumOrMax));
    }
}
