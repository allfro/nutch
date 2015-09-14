package org.apache.nutch.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.io.LinkNode;
import org.apache.nutch.io.LinkNodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Merges LinkNode objects into a single array value per url. This allows all
 * values to be quickly retrieved and printed via the Reader tool.
 */
public class LinkDbMergerReducer extends
        Reducer<Text, LinkNode, Text, LinkNodes> {

    private Configuration conf;
    private static final int maxInlinks = 50000;

    @Override
    /**
     * Aggregate all LinkNode objects for a given url.
     */
    protected void reduce(Text key, Iterable<LinkNode> values, Context context) throws IOException, InterruptedException {
        List<LinkNode> nodeList = new ArrayList<LinkNode>();
        int numNodes = 0;

        for (LinkNode cur : values) {
            if (numNodes < maxInlinks) {
                nodeList.add(WritableUtils.clone(cur, conf));
                numNodes++;
            } else {
                break;
            }
        }

        LinkNode[] linkNodesAr = nodeList.toArray(new LinkNode[nodeList.size()]);
        LinkNodes linkNodes = new LinkNodes(linkNodesAr);
        context.write(key, linkNodes);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.conf = context.getConfiguration();
    }

}
