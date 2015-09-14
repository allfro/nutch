package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.Inlink;
import org.apache.nutch.io.Inlinks;
import org.apache.nutch.io.Outlink;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.ParseData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by ndouba on 9/2/15.
 */
public class LinkDbMapper extends Mapper<Text, ParseData, Text, Inlinks> {

    public static final Logger LOG = LoggerFactory.getLogger(LinkDbMapper.class);

    public static final String IGNORE_INTERNAL_LINKS = "db.ignore.internal.links";
    public static final String IGNORE_EXTERNAL_LINKS = "db.ignore.external.links";

    public static final String CURRENT_NAME = "current";
    public static final String LOCK_NAME = ".locked";

    private int maxAnchorLength;
    private boolean ignoreInternalLinks;
    private boolean ignoreExternalLinks;
    private URLFilters urlFilters;
    private URLNormalizers urlNormalizers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        maxAnchorLength = configuration.getInt("db.max.anchor.length", 100);
        ignoreInternalLinks = configuration.getBoolean(IGNORE_INTERNAL_LINKS, true);
        ignoreExternalLinks = configuration.getBoolean(IGNORE_EXTERNAL_LINKS, false);

        if (configuration.getBoolean(LinkDbFilterMapper.URL_FILTERING, false)) {
            urlFilters = new URLFilters(configuration);
        }
        if (configuration.getBoolean(LinkDbFilterMapper.URL_NORMALIZING, false)) {
            urlNormalizers = new URLNormalizers(configuration, URLNormalizers.SCOPE_LINKDB);
        }
    }

    @Override
    protected void map(Text key, ParseData parseData, Context context) throws IOException, InterruptedException {
        String fromUrl = key.toString();
        String fromHost = getHost(fromUrl);
        if (urlNormalizers != null) {
            try {
                fromUrl = urlNormalizers
                        .normalize(fromUrl, URLNormalizers.SCOPE_LINKDB); // normalize the
                // url
            } catch (Exception e) {
                LOG.warn("Skipping {}: {}", fromUrl, e);
                fromUrl = null;
            }
        }
        if (fromUrl != null && urlFilters != null) {
            try {
                fromUrl = urlFilters.filter(fromUrl); // filter the url
            } catch (Exception e) {
                LOG.warn("Skipping {}: {}", fromUrl, e);
                fromUrl = null;
            }
        }
        if (fromUrl == null)
            return; // discard all outlinks
        Outlink[] outlinks = parseData.getOutlinks();
        Inlinks inlinks = new Inlinks();
        for (Outlink outlink : outlinks) {
            String toUrl = outlink.getToUrl();

            if (ignoreInternalLinks) {
                String toHost = getHost(toUrl);
                if (toHost == null || toHost.equals(fromHost)) { // internal link
                    continue; // skip it
                }
            } else if (ignoreExternalLinks) {
                String toHost = getHost(toUrl);
                if (toHost == null || !toHost.equals(fromHost)) { // external link
                    continue;                               // skip it
                }
            }
            if (urlNormalizers != null) {
                try {
                    toUrl = urlNormalizers.normalize(toUrl, URLNormalizers.SCOPE_LINKDB); // normalize
                    // the
                    // url
                } catch (Exception e) {
                    LOG.warn("Skipping {}: {}", toUrl, e);
                    toUrl = null;
                }
            }
            if (toUrl != null && urlFilters != null) {
                try {
                    toUrl = urlFilters.filter(toUrl); // filter the url
                } catch (Exception e) {
                    LOG.warn("Skipping {}: {}", toUrl, e);
                    toUrl = null;
                }
            }
            if (toUrl == null)
                continue;
            inlinks.clear();
            String anchor = outlink.getAnchor(); // truncate long anchors
            if (anchor.length() > maxAnchorLength) {
                anchor = anchor.substring(0, maxAnchorLength);
            }
            inlinks.add(new Inlink(fromUrl, anchor)); // collect inverted link
            context.write(new Text(toUrl), inlinks);
        }
    }

    private String getHost(String url) {
        try {
            return new URL(url).getHost().toLowerCase();
        } catch (MalformedURLException e) {
            return null;
        }
    }
}
