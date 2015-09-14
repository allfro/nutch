package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.io.LinkDatum;
import org.apache.nutch.io.NutchWritable;
import org.apache.nutch.io.Outlink;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.scoring.webgraph.WebGraph;
import org.apache.nutch.util.URLUtil;

import java.io.IOException;
import java.util.*;

/**
 * The OutlinkDb creates a database of all outlinks. Outlinks to internal urls
 * by domain and host can be ignored. The number of Outlinks out to a given
 * page or domain can also be limited.
 */
public class OutlinkDbMapper extends Mapper<Text, Writable, Text, NutchWritable> {

    public static final String URL_NORMALIZING = "webgraph.url.normalizers";
    public static final String URL_FILTERING = "webgraph.url.filters";


    // using normalizers and/or filters
    private boolean normalize = false;
    private boolean filter = false;

    // url normalizers, filters and job configuration
    private URLNormalizers urlNormalizers;
    private URLFilters filters;
    private Configuration conf;

    /**
     * Normalizes and trims extra whitespace from the given url.
     *
     * @param url
     *          The url to normalize.
     *
     * @return The normalized url.
     */
    private String normalizeUrl(String url) {

        if (!normalize) {
            return url;
        }

        String normalized = null;
        if (urlNormalizers != null) {
            try {

                // normalize and trim the url
                normalized = urlNormalizers.normalize(url,
                        URLNormalizers.SCOPE_DEFAULT);
                normalized = normalized.trim();
            } catch (Exception e) {
                WebGraph.LOG.warn("Skipping " + url + ":" + e);
                normalized = null;
            }
        }
        return normalized;
    }

    /**
     * Filters the given url.
     *
     * @param url
     *          The url to filter.
     *
     * @return The filtered url or null.
     */
    private String filterUrl(String url) {

        if (!filter) {
            return url;
        }

        try {
            url = filters.filter(url);
        } catch (Exception e) {
            url = null;
        }

        return url;
    }

    /**
     * Returns the fetch time from the parse data or the current system time if
     * the fetch time doesn't exist.
     *
     * @param data
     *          The parse data.
     *
     * @return The fetch time as a long.
     */
    private long getFetchTime(ParseData data) {

        // default to current system time
        long fetchTime = System.currentTimeMillis();
        String fetchTimeStr = data.getContentMeta().get(Nutch.FETCH_TIME_KEY);
        try {
            // get the fetch time from the parse data
            fetchTime = Long.parseLong(fetchTimeStr);
        } catch (Exception e) {
            fetchTime = System.currentTimeMillis();
        }
        return fetchTime;
    }

    @Override
    /**
     * Configures the OutlinkDb job. Sets up internal links and link limiting.
     */
    public void setup(Context context) {
        this.conf = context.getConfiguration();

        normalize = conf.getBoolean(URL_NORMALIZING, false);
        filter = conf.getBoolean(URL_FILTERING, false);

        if (normalize) {
            urlNormalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_DEFAULT);
        }

        if (filter) {
            filters = new URLFilters(conf);
        }
    }

    /**
     * Passes through existing LinkDatum objects from an existing OutlinkDb and
     * maps out new LinkDatum objects from new crawls ParseData.
     */
    public void map(Text key, Writable value,Context context)
            throws IOException, InterruptedException {

        // normalize url, stop processing if null
        String url = normalizeUrl(key.toString());
        if (url == null) {
            return;
        }

        // filter url
        if (filterUrl(url) == null) {
            return;
        }

        // Overwrite the key with the normalized URL
        key.set(url);

        if (value instanceof CrawlDatum) {
            CrawlDatum datum = (CrawlDatum) value;

            if (datum.getStatus() == CrawlDatum.STATUS_FETCH_REDIR_TEMP
                    || datum.getStatus() == CrawlDatum.STATUS_FETCH_REDIR_PERM
                    || datum.getStatus() == CrawlDatum.STATUS_FETCH_GONE) {

                // Tell the reducer to get rid of all instances of this key
                context.write(key, new NutchWritable(new BooleanWritable(true)));
            }
        } else if (value instanceof ParseData) {
            // get the parse data and the outlinks from the parse data, along with
            // the fetch time for those links
            ParseData data = (ParseData) value;
            long fetchTime = getFetchTime(data);
            Outlink[] outlinkAr = data.getOutlinks();
            Map<String, String> outlinkMap = new LinkedHashMap<String, String>();

            // normalize urls and put into map
            if (outlinkAr != null && outlinkAr.length > 0) {
                for (Outlink outlink : outlinkAr) {
                    String toUrl = normalizeUrl(outlink.getToUrl());

                    if (filterUrl(toUrl) == null) {
                        continue;
                    }

                    // only put into map if the url doesn't already exist in the map or
                    // if it does and the anchor for that link is null, will replace if
                    // url is existing
                    boolean existingUrl = outlinkMap.containsKey(toUrl);
                    if (toUrl != null
                            && (!existingUrl || (existingUrl && outlinkMap.get(toUrl) == null))) {
                        outlinkMap.put(toUrl, outlink.getAnchor());
                    }
                }
            }

            // collect the outlinks under the fetch time
            for (String outlinkUrl : outlinkMap.keySet()) {
                String anchor = outlinkMap.get(outlinkUrl);
                LinkDatum datum = new LinkDatum(outlinkUrl, anchor, fetchTime);
                context.write(key, new NutchWritable(datum));
            }
        } else if (value instanceof LinkDatum) {
            LinkDatum datum = (LinkDatum) value;
            String linkDatumUrl = normalizeUrl(datum.getUrl());

            if (filterUrl(linkDatumUrl) != null) {
                datum.setUrl(linkDatumUrl);

                // collect existing outlinks from existing OutlinkDb
                context.write(key, new NutchWritable(datum));
            }
        }
    }

}
