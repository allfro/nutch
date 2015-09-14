package org.apache.nutch.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.io.LinkDatum;
import org.apache.nutch.io.LoopSet;
import org.apache.nutch.io.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by ndouba on 9/2/15.
 */
public class InverterReducer extends Reducer<Text, ObjectWritable, Text, LinkDatum> {

    private static final Logger LOG = LoggerFactory.getLogger(InverterReducer.class);
    private Configuration configuration;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.configuration = context.getConfiguration();
    }

    /**
     * Inverts outlinks to inlinks, attaches current score for the outlink from
     * the NodeDb of the WebGraph and removes any outlink that is contained
     * within the loopset.
     */
    @Override
    protected void reduce(Text key, Iterable<ObjectWritable> values, Context context)
            throws IOException, InterruptedException {

        String fromUrl = key.toString();
        List<LinkDatum> outlinks = new ArrayList<>();
        Node node = null;
        LoopSet loops = null;

        // aggregate outlinks, assign other values
        for (ObjectWritable write: values) {
            Object obj = write.get();
            if (obj instanceof Node) {
                node = (Node) obj;
            } else if (obj instanceof LinkDatum) {
                outlinks.add(WritableUtils.clone((LinkDatum) obj, configuration));
            } else if (obj instanceof LoopSet) {
                loops = (LoopSet) obj;
            }
        }

        // Check for the possibility of a LoopSet object without Node and
        // LinkDatum objects. This can happen
        // with webgraphs that receive deletes (e.g. link.delete.gone and/or URL
        // filters or normalizers) but
        // without an updated Loops database.
        // See: https://issues.apache.org/jira/browse/NUTCH-1299
        if (node == null && loops != null) {
            // Nothing to do
            LOG.warn("LoopSet without Node object received for {} . You should either not use " +
                    "Loops as input of the LinkRank program or rerun the Loops program over the WebGraph.", key.toString());
            return;
        }

        // get the number of outlinks and the current inlink and outlink scores
        // from the node of the url
        int numOutlinks = node.getNumOutlinks();
        float inlinkScore = node.getInlinkScore();
        float outlinkScore = node.getOutlinkScore();
        LOG.debug("{}: num outlinks {}", fromUrl, numOutlinks);

        // can't invert if no outlinks
        if (numOutlinks > 0) {

            Set<String> loopSet = (loops != null) ? loops.getLoopSet() : null;
            for (LinkDatum outlink : outlinks) {
                String toUrl = outlink.getUrl();

                // remove any url that is contained in the loopset
                if (loopSet != null && loopSet.contains(toUrl)) {
                    LOG.debug("{}: Skipping inverting inlink from loop {}", fromUrl, toUrl);
                    continue;
                }
                outlink.setUrl(fromUrl);
                outlink.setScore(outlinkScore);

                // collect the inverted outlink
                context.write(new Text(toUrl), outlink);
                LOG.debug("{}: inverting inlink from {} origscore: {} numOutlinks: {} outlinkscore: {}",
                        toUrl, fromUrl, inlinkScore, numOutlinks, outlinkScore);
            }
        }
    }
}
