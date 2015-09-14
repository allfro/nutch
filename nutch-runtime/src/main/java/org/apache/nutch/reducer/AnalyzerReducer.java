package org.apache.nutch.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.io.LinkDatum;
import org.apache.nutch.io.Node;
import org.apache.nutch.scoring.webgraph.LinkRank;
import org.apache.nutch.util.URLUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by ndouba on 9/2/15.
 */
public class AnalyzerReducer extends Reducer<Text, ObjectWritable, Text, Node> {

    private Configuration conf;
    private float dampingFactor = 0.85f;
    private float rankOne = 0.0f;
    private int itNum = 0;
    private boolean limitPages = true;
    private boolean limitDomains = true;

    @Override
    public void setup(Context context) {
        try {
            this.conf = context.getConfiguration();
            this.dampingFactor = conf
                    .getFloat("link.analyze.damping.factor", 0.85f);
            this.rankOne = conf.getFloat("link.analyze.rank.one", 0.0f);
            this.itNum = conf.getInt("link.analyze.iteration", 0);
            limitPages = conf.getBoolean("link.ignore.limit.page", true);
            limitDomains = conf.getBoolean("link.ignore.limit.domain", true);
        } catch (Exception e) {
            LinkRank.LOG.error(StringUtils.stringifyException(e));
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    /**
     * Performs a single iteration of link analysis. The resulting scores are
     * stored in a temporary NodeDb which replaces the NodeDb of the WebGraph.
     */
    protected void reduce(Text key, Iterable<ObjectWritable> values, Context context)
            throws IOException, InterruptedException {

        String url = key.toString();
        Set<String> domains = new HashSet<String>();
        Set<String> pages = new HashSet<String>();
        Node node = null;

        // a page with zero inlinks has a score of rankOne
        int numInlinks = 0;
        float totalInlinkScore = rankOne;

        for (ObjectWritable next : values) {
            Object value = next.get();
            if (value instanceof Node) {
                node = (Node) value;
            } else if (value instanceof LinkDatum) {

                LinkDatum linkDatum = (LinkDatum) value;
                float scoreFromInlink = linkDatum.getScore();
                String inlinkUrl = linkDatum.getUrl();
                String inLinkDomain = URLUtil.getDomainName(inlinkUrl);
                String inLinkPage = URLUtil.getPage(inlinkUrl);

                // limit counting duplicate inlinks by pages or domains
                if ((limitPages && pages.contains(inLinkPage))
                        || (limitDomains && domains.contains(inLinkDomain))) {
                    LinkRank.LOG.debug(url + ": ignoring " + scoreFromInlink + " from "
                            + inlinkUrl + ", duplicate page or domain");
                    continue;
                }

                // aggregate total inlink score
                numInlinks++;
                totalInlinkScore += scoreFromInlink;
                domains.add(inLinkDomain);
                pages.add(inLinkPage);
                LinkRank.LOG.debug(url + ": adding " + scoreFromInlink + " from " + inlinkUrl
                        + ", total: " + totalInlinkScore);
            }
        }

        // calculate linkRank score formula
        float linkRankScore = (1 - this.dampingFactor)
                + (this.dampingFactor * totalInlinkScore);

        LinkRank.LOG.debug(url + ": score: " + linkRankScore + " num inlinks: "
                + numInlinks + " iteration: " + itNum);

        // store the score in a temporary NodeDb
        Node outNode = WritableUtils.clone(node, conf);
        outNode.setInlinkScore(linkRankScore);
        context.write(key, outNode);
    }
}
