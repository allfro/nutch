package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.Node;

import java.io.IOException;

/**
 * Outputs the top urls sorted in descending order. Depending on the flag set
 * on the command line, the top urls could be for number of inlinks, for
 * number of outlinks, or for link analysis score.
 */
public class NodeDumperSorterMapper extends
        Mapper<Text, Node, FloatWritable, Text> {

    private boolean inlinks = false;
    private boolean outlinks = false;


    /**
     * Configures the job, sets the flag for type of content and the topN number
     * if any.
     */
    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
        this.inlinks = configuration.getBoolean("inlinks", false);
        this.outlinks = configuration.getBoolean("outlinks", false);
    }


    @Override
    /**
     * Outputs the url with the appropriate number of inlinks, outlinks, or for
     * score.
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

        // number collected with negative to be descending
        context.write(new FloatWritable(-number), key);
    }
}
