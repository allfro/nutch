package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/** Normalize and filter injected urls. */
public class InjectorMapper extends
        Mapper<WritableComparable<?>, Text, Text, CrawlDatum> {
    private URLNormalizers urlNormalizers;
    private int interval;
    private float scoreInjected;
    private URLFilters filters;
    private ScoringFilters scoringFilters;
    private long currentTime;
    private Counter urlsFiltered;
    private Counter urlsInjected;


    private static final Logger LOG = LoggerFactory.getLogger(InjectorMapper.class);

    /** metadata key reserved for setting a custom score for a specific URL */
    public static String METADATA_NUTCH_SCORE = "nutch.score";
    /**
     * metadata key reserved for setting a custom fetchInterval for a specific URL
     */
    public static String METADATA_FETCH_INTERVAL = "nutch.fetchInterval";
    /**
     * metadata key reserved for setting a fixed custom fetchInterval for a
     * specific URL
     */
    public static String METADATA_FIXED_FETCH_INTERVAL = "nutch.fetchInterval.fixed";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        urlNormalizers = new URLNormalizers(configuration, URLNormalizers.SCOPE_INJECT);
        interval = configuration.getInt("db.fetch.interval.default", 2592000);
        filters = new URLFilters(configuration);
        scoringFilters = new ScoringFilters(configuration);
        scoreInjected = configuration.getFloat("db.score.injected", 1.0f);
        currentTime = configuration.getLong("injector.current.time", System.currentTimeMillis());
        urlsFiltered = context.getCounter("injector", "urls_filtered");
        urlsInjected = context.getCounter("injector", "urls_injected");
    }

    @Override
    protected void map(WritableComparable<?> key, Text value, Context context) throws IOException, InterruptedException {
        String url = value.toString().trim(); // value is line of text

        if ((url.length() == 0 || url.startsWith("#"))) {
      /* Ignore line that start with # */
            return;
        }

        // if tabs : metadata that could be stored
        // must be name=value and separated by \t
        float customScore = -1f;
        int customInterval = interval;
        int fixedInterval = -1;

        Map<String, String> metadata = new TreeMap<String, String>();

        // Check if our URL entry specifies any crawling parameters.
        // Valid parameters are nutch.score, nutch.fetchinterval, and nutch.fetchinterval.fixed.
        // Seed entries with metadata parameters are tab delimited and have the following format:
        // <url>    <metadataName1>=<metadataValue1>    ... <metadataNameN>=<metadataValueN>
        if (url.contains("\t")) {
            String[] splits = url.split("\t");
            url = splits[0];
            for (int s = 1; s < splits.length; s++) {
                // find separation between name and value
                int indexEquals = splits[s].indexOf("=");
                if (indexEquals == -1) {
                    // skip anything without a =
                    continue;
                }
                String metaName = splits[s].substring(0, indexEquals);
                String metaValue = splits[s].substring(indexEquals + 1);
                if (metaName.equals(METADATA_NUTCH_SCORE)) {
                    try {
                        customScore = Float.parseFloat(metaValue);
                    } catch (NumberFormatException ignored) {
                    }
                } else if (metaName.equals(METADATA_FETCH_INTERVAL)) {
                    try {
                        customInterval = Integer.parseInt(metaValue);
                    } catch (NumberFormatException ignored) {
                    }
                } else if (metaName.equals(METADATA_FIXED_FETCH_INTERVAL)) {
                    try {
                        fixedInterval = Integer.parseInt(metaValue);
                    } catch (NumberFormatException ignored) {
                    }
                } else {
                    metadata.put(metaName, metaValue);
                }
            }
        }

        // Try normalizing the URL
        try {
            url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INJECT);
            url = filters.filter(url); // filter the url
        } catch (Exception e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Skipping " + url + ":" + e);
            }
            url = null;
        }

        if (url == null) { // URL normalization failed, so increment our fail counter and drop the URL from the crawl
            urlsFiltered.increment(1);
        } else { // if it passes
            value.set(url); // collect it
            CrawlDatum datum = new CrawlDatum();
            datum.setStatus(CrawlDatum.STATUS_INJECTED);

            // Is interval custom? Then set as meta data
            if (fixedInterval != -1) {
                // Set writable using float. Float is used by
                // AdaptiveFetchSchedule
                datum.getMetaData().put(Nutch.WRITABLE_FIXED_INTERVAL_KEY,
                        new FloatWritable(fixedInterval));
                datum.setFetchInterval(fixedInterval);
            } else {
                datum.setFetchInterval(customInterval);
            }

            datum.setFetchTime(currentTime);

            // now add the metadata
            for (String metadataKey : metadata.keySet()) {
                datum.getMetaData().put(new Text(metadataKey), new Text(metadata.get(metadataKey)));
            }

            datum.setScore((customScore != -1)?customScore:scoreInjected);

            try {
                scoringFilters.injectedScore(value, datum);
            } catch (ScoringFilterException e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Cannot filter injected score for url {} using default ({})",
                            url,
                            e.getMessage()
                    );
                }
            }

            // Increment our URLs injected counter
            urlsInjected.increment(1);

            context.write(value, datum);
        }
    }
}
