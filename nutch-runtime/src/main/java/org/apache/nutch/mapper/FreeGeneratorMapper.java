package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.io.SelectorEntryWritable;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.tools.FreeGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FreeGeneratorMapper extends
        Mapper<WritableComparable<?>, Text, Text, SelectorEntryWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(FreeGeneratorMapper.class);
    private URLNormalizers normalizers = null;
    private URLFilters filters = null;
    private ScoringFilters scfilters;
    private CrawlDatum datum = new CrawlDatum();
    private Text url = new Text();
    private int defaultInterval = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        defaultInterval = configuration.getInt("db.fetch.interval.default", 0);
        scfilters = new ScoringFilters(configuration);
        if (configuration.getBoolean(FreeGenerator.FILTER_KEY, false)) {
            filters = new URLFilters(configuration);
        }
        if (configuration.getBoolean(FreeGenerator.NORMALIZE_KEY, false)) {
            normalizers = new URLNormalizers(configuration, URLNormalizers.SCOPE_INJECT);
        }
    }

    @Override
    protected void map(WritableComparable<?> key, Text value, Context context) throws IOException, InterruptedException {
        // value is a line of text
        SelectorEntryWritable entry = new SelectorEntryWritable();
        String urlString = value.toString();
        try {
            if (normalizers != null) {
                urlString = normalizers.normalize(urlString,
                        URLNormalizers.SCOPE_INJECT);
            }
            if (urlString != null && filters != null) {
                urlString = filters.filter(urlString);
            }
            if (urlString != null) {
                url.set(urlString);
                scfilters.injectedScore(url, datum);
            }
        } catch (Exception e) {
            LOG.warn("Error adding url '" + value.toString() + "', skipping: "
                    + StringUtils.stringifyException(e));
            return;
        }
        if (urlString == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("- skipping {}", value.toString());
            }
            return;
        }
        entry.datum = datum;
        entry.url = url;
        // https://issues.apache.org/jira/browse/NUTCH-1430
        entry.datum.setFetchInterval(defaultInterval);
        context.write(url, entry);
    }


}
