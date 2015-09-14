package org.apache.nutch.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.io.LoopSet;
import org.apache.nutch.io.Route;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class LoopFinalizerReducer extends Reducer<Text, Route, Text, LoopSet> {

    @Override
    /**
     * Aggregates all found routes for a given start url into a loopset and
     * collects the loopset.
     */
    public void reduce(Text key, Iterable<Route> values, Context context)
            throws IOException, InterruptedException {

        LoopSet loops = new LoopSet();
        for (Route route: values) {
            loops.getLoopSet().add(route.getOutlinkUrl());
        }
        context.write(key, loops);
    }

}
