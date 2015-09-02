package org.apache.nutch.reducer;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.scoring.webgraph.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ScoreUpdaterReducer extends Reducer<Text, ObjectWritable, Text, CrawlDatum>  {

    private static final Logger LOG = LoggerFactory.getLogger(ScoreUpdaterReducer.class);

    private float clearScore = 0.0f;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        clearScore = context.getConfiguration().getFloat("link.score.updater.clear.score", 0.0f);
    }

    @Override
    protected void reduce(Text key, Iterable<ObjectWritable> values, Context context)
            throws IOException, InterruptedException {

        String url = key.toString();
        Node node = null;
        CrawlDatum datum = null;

        // set the node and the crawl datum, should be one of each unless no node
        // for url in the crawldb

        for (ObjectWritable next : values) {
            Object value = next.get();
            if (value instanceof Node) {
                node = (Node) value;
            } else if (value instanceof CrawlDatum) {
                datum = (CrawlDatum) value;
            }
        }

        // datum should never be null, could happen if somehow the url was
        // normalized or changed after being pulled from the crawldb
        if (datum != null) {

            if (node != null) {
                // set the inlink score in the nodedb
                float inlinkScore = node.getInlinkScore();
                datum.setScore(inlinkScore);
                LOG.debug("{}: setting to score {}", url, inlinkScore);
            } else {
                // clear out the score in the crawldb
                datum.setScore(clearScore);
                LOG.debug("{}: setting to clear score of {}", url, clearScore);
            }

            context.write(key, datum);
        } else {
            LOG.debug("{}: no datum", url);
        }
    }
}
