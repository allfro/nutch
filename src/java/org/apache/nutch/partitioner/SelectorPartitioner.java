package org.apache.nutch.partitioner;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.nutch.io.SelectorEntryWritable;

/**
 * Created by ndouba on 15-08-28.
 */
public class SelectorPartitioner extends Partitioner<FloatWritable, Writable> implements Configurable {

    private final URLPartitioner partitioner = new URLPartitioner();

    @Override
    public int getPartition(FloatWritable key, Writable value, int numPartitions) {
        return partitioner.getPartition(((SelectorEntryWritable) value).url, key, numPartitions);
    }

    @Override
    public void setConf(Configuration conf) {
        partitioner.setConf(conf);
    }

    @Override
    public Configuration getConf() {
        return partitioner.getConf();
    }
}
