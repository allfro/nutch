package org.apache.nutch.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.Node;
import org.apache.nutch.scoring.webgraph.LinkRank;

import java.io.IOException;

/**
 * The Counter job that determines the total number of nodes in the WebGraph.
 * This is used to determine a rank one score for pages with zero inlinks but
 * that contain outlinks.
 */
public class CounterMapper extends
        Mapper<Text, Node, Text, LongWritable> {

    private static Text numNodes = new Text(LinkRank.NUM_NODES);
    private static LongWritable one = new LongWritable(1L);

    @Override
    protected void map(Text key, Node value, Context context) throws IOException, InterruptedException {
        context.write(numNodes, one);
    }
}
