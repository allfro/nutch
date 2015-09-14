package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.Node;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class InitializerMapper extends Mapper<Text, Node, Text, Node> {

    private float initialScore = 1.0f;
    private Configuration configuration;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        configuration = context.getConfiguration();
        initialScore = configuration.getFloat("link.analyze.initial.score", 1.0f);
    }

    @Override
    protected void map(Text key, Node node, Context context) throws IOException, InterruptedException {
        String url = key.toString();
        Node outNode = WritableUtils.clone(node, configuration);
        outNode.setInlinkScore(initialScore);

        context.write(new Text(url), outNode);
    }

}
