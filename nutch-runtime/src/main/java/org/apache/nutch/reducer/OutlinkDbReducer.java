package org.apache.nutch.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.io.LinkDatum;
import org.apache.nutch.io.NutchWritable;
import org.apache.nutch.util.URLUtil;

import java.io.IOException;
import java.util.*;

/**
 * Created by ndouba on 9/2/15.
 */
public class OutlinkDbReducer extends Reducer<Text, NutchWritable, Text, LinkDatum> {

    private Configuration conf;
    private Counter addedLinks;
    private Counter removedLinks;

    // ignoring internal domains, internal hosts
    private boolean ignoreDomain = true;
    private boolean ignoreHost = true;

    // limiting urls out to a page or to a domain
    private boolean limitPages = true;
    private boolean limitDomains = true;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.conf = context.getConfiguration();
        ignoreHost = conf.getBoolean("link.ignore.internal.host", true);
        ignoreDomain = conf.getBoolean("link.ignore.internal.domain", true);
        limitPages = conf.getBoolean("link.ignore.limit.page", true);
        limitDomains = conf.getBoolean("link.ignore.limit.domain", true);
        addedLinks = context.getCounter("WebGraph.outlinks", "added links");
        removedLinks = context.getCounter("WebGraph.outlinks", "removed links");
    }

    @Override
    public void reduce(Text key, Iterable<NutchWritable> values, Context context)
            throws IOException, InterruptedException {

    // aggregate all outlinks, get the most recent timestamp for a fetch
    // which should be the timestamp for all of the most recent outlinks
    long mostRecent = 0L;
    List<LinkDatum> outlinkList = new ArrayList<LinkDatum>();
    for (Writable value: values) {
        value = ((NutchWritable)value).get();

        if (value instanceof LinkDatum) {
            // loop through, change out most recent timestamp if needed
            LinkDatum next = (LinkDatum) value;
            long timestamp = next.getTimestamp();
            if (mostRecent == 0L || mostRecent < timestamp) {
                mostRecent = timestamp;
            }
            outlinkList.add(WritableUtils.clone(next, conf));
            addedLinks.increment(1);
        } else if (value instanceof BooleanWritable) {
            BooleanWritable delete = (BooleanWritable) value;
            // Actually, delete is always true, otherwise we don't emit it in the
            // mapper in the first place
            if (delete.get()) {
                // This page is gone, do not emit it's outlinks
                removedLinks.increment(1);
                return;
            }
        }
    }

    // get the url, domain, and host for the url
    String url = key.toString();
    String domain = URLUtil.getDomainName(url);
    String host = URLUtil.getHost(url);

    // setup checking sets for domains and pages
    Set<String> domains = new HashSet<String>();
    Set<String> pages = new HashSet<String>();

    // loop through the link datums
    for (LinkDatum datum : outlinkList) {

        // get the url, host, domain, and page for each outlink
        String toUrl = datum.getUrl();
        String toDomain = URLUtil.getDomainName(toUrl);
        String toHost = URLUtil.getHost(toUrl);
        String toPage = URLUtil.getPage(toUrl);
        datum.setLinkType(LinkDatum.OUTLINK);

        // outlinks must be the most recent and conform to internal url and
        // limiting rules, if it does collect it
        if (datum.getTimestamp() == mostRecent
                && (!limitPages || (limitPages && !pages.contains(toPage)))
                && (!limitDomains || (limitDomains && !domains.contains(toDomain)))
                && (!ignoreHost || (ignoreHost && toHost != null && !toHost.equalsIgnoreCase(host)))
                && (!ignoreDomain || (ignoreDomain && !toDomain.equalsIgnoreCase(domain)))) {
            context.write(key, datum);
            pages.add(toPage);
            domains.add(toDomain);
        }
    }
}
}
