package org.apache.nutch.reducer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.io.LinkDatum;
import org.apache.nutch.io.Node;

import java.io.IOException;
import java.util.Iterator;

/**
 * Creates the Node database which consists of the number of in and outlinks
 * for each url and a score slot for analysis programs such as LinkRank.
 */
public class NodeDbReducer extends Reducer<Text, LinkDatum, Text, Node> {

    /**
     * Counts the number of inlinks and outlinks for each url and sets a default
     * score of 0.0 for each url (node) in the webgraph.
     */
    public void reduce(Text key, Iterable<LinkDatum> values, Context context)
            throws IOException, InterruptedException {

        Node node = new Node();
        int numInlinks = 0;
        int numOutlinks = 0;

        // loop through counting number of in and out links
        for (LinkDatum next: values) {
            if (next.getLinkType() == LinkDatum.INLINK) {
                numInlinks++;
            } else if (next.getLinkType() == LinkDatum.OUTLINK) {
                numOutlinks++;
            }
        }

        // set the in and outlinks and a default score of 0
        node.setNumInlinks(numInlinks);
        node.setNumOutlinks(numOutlinks);
        node.setInlinkScore(0.0f);
        context.write(key, node);
    }
}
