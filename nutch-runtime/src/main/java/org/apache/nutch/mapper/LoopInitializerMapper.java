package org.apache.nutch.mapper;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Initializes the Loop routes.
 */
public class LoopInitializerMapper extends Mapper<Text, Writable, Text, ObjectWritable> {

    @Override
    /**
     * Wraps values in ObjectWritable.
     */
    protected void map(Text key, Writable value, Context context) throws IOException, InterruptedException {
        ObjectWritable objWrite = new ObjectWritable();
        objWrite.set(value);
        context.write(key, objWrite);
    }

}
