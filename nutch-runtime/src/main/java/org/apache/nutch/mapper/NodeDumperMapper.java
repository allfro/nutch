package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.Node;
import org.apache.nutch.util.URLUtil;

import java.io.IOException;

/**
 * Outputs the hosts or domains with an associated value. This value consists
 * of either the number of inlinks, the number of outlinks or the score. The
 * computed value is then either the sum of all parts or the top value.
 */
public class NodeDumperMapper extends Mapper<Text, Node, Text, FloatWritable> {

    private boolean inlinks = false;
    private boolean outlinks = false;
    private boolean host = false;

    @Override
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        this.inlinks = conf.getBoolean("inlinks", false);
        this.outlinks = conf.getBoolean("outlinks", false);
        this.host = conf.getBoolean("host", false);
    }

    @Override
    /**
     * Outputs the host or domain as key for this record and numInlinks,
     * numOutlinks or score as the value.
     */
    public void map(Text key, Node node, Context context)
            throws IOException, InterruptedException {

        float number = 0;
        if (inlinks) {
            number = node.getNumInlinks();
        } else if (outlinks) {
            number = node.getNumOutlinks();
        } else {
            number = node.getInlinkScore();
        }

        if (host) {
            key.set(URLUtil.getHost(key.toString()));
        } else {
            key.set(URLUtil.getDomainName(key.toString()));
        }

        context.write(key, new FloatWritable(number));
    }

}
