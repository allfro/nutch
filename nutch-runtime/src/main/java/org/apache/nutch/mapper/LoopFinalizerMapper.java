package org.apache.nutch.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.Route;

import java.io.IOException;

/**
 * Finishes the Loops job by aggregating and collecting and found routes.
 */
public class LoopFinalizerMapper extends Mapper<Text, Route, Text, Route> {

    @Override
    /**
     * Maps out and found routes, those will be the link cycles.
     */
    public void map(Text key, Route value, Context context) throws IOException, InterruptedException {

        if (value.isFound()) {
            String lookingFor = value.getLookingFor();
            context.write(new Text(lookingFor), value);
        }
    }
}
