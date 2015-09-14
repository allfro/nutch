package org.apache.nutch.crawl;

import org.apache.hadoop.util.PriorityQueue;
import org.apache.nutch.io.CrawlDatum;

/**
 * Created by ndouba on 9/2/15.
 */
public class InlinkPriorityQueue extends PriorityQueue<CrawlDatum> {

    public InlinkPriorityQueue(int maxSize) {
        initialize(maxSize);
    }

    /** Determines the ordering of objects in this priority queue. **/
    protected boolean lessThan(Object arg0, Object arg1) {
        CrawlDatum candidate = (CrawlDatum) arg0;
        CrawlDatum least = (CrawlDatum) arg1;
        return candidate.getScore() > least.getScore();
    }

}
