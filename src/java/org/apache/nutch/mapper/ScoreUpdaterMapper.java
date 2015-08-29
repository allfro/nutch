package org.apache.nutch.mapper;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ndouba on 15-08-28.
 */
public class ScoreUpdaterMapper extends Mapper<Text, Writable, Text, ObjectWritable> {

    @Override
    protected void map(Text key, Writable value, Context context) throws IOException, InterruptedException {
        ObjectWritable objWrite = new ObjectWritable();
        objWrite.set(value);
        context.write(key, objWrite);
    }

}
