package org.apache.nutch.mapper;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Inverts outlinks and attaches current score from the NodeDb of the
 * WebGraph. The link analysis process consists of inverting, analyzing and
 * scoring, in a loop for a given number of iterations.
 */
public class InverterMapper extends
        Mapper<Text, Writable, Text, ObjectWritable> {

    @Override
    /**
     * Convert values to ObjectWritable
     */
    public void map(Text key, Writable value, Context context)
            throws IOException, InterruptedException {

        ObjectWritable objWrite = new ObjectWritable();
        objWrite.set(value);
        context.write(key, objWrite);
    }
}
