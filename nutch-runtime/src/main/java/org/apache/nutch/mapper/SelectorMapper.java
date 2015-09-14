package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.io.CrawlDatum;
import org.apache.hadoop.io.MapWritable;
import org.apache.nutch.io.SelectorEntryWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Selects entries due for fetch. */
public class SelectorMapper extends
        Mapper<Text, CrawlDatum, FloatWritable, SelectorEntryWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(SelectorMapper.class);
    private LongWritable genTime = new LongWritable(System.currentTimeMillis());
    private long curTime;
    private URLFilters filters;
    private ScoringFilters scfilters;
    private SelectorEntryWritable entry = new SelectorEntryWritable();
    private FloatWritable sortValue = new FloatWritable();
    private boolean filter;
    private long genDelay;
    private FetchSchedule schedule;
    private float scoreThreshold = 0f;
    private int intervalThreshold = -1;
    private String restrictStatus = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        curTime = configuration.getLong(Generator.GENERATOR_CUR_TIME, System.currentTimeMillis());
        filters = new URLFilters(configuration);
        scfilters = new ScoringFilters(configuration);
        filter = configuration.getBoolean(Generator.GENERATOR_FILTER, true);
        genDelay = configuration.getLong(Generator.GENERATOR_DELAY, 7L) * 3600L * 24L * 1000L;
        long time = configuration.getLong(Nutch.GENERATE_TIME_KEY, 0L);
        if (time > 0)
            genTime.set(time);
        schedule = FetchScheduleFactory.getFetchSchedule(configuration);
        scoreThreshold = configuration.getFloat(Generator.GENERATOR_MIN_SCORE, Float.NaN);
        intervalThreshold = configuration.getInt(Generator.GENERATOR_MIN_INTERVAL, -1);
        restrictStatus = configuration.get(Generator.GENERATOR_RESTRICT_STATUS, null);
    }

    @Override
    protected void map(Text url, CrawlDatum crawlDatum, Context context) throws IOException, InterruptedException {
        if (filter) {
            // If filtering is on don't generate URLs that don't pass
            // URLFilters
            try {
                if (filters.filter(url.toString()) == null)
                    return;
            } catch (URLFilterException e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Couldn't filter url: {} ({})", url, e.getMessage());
                }
            }
        }

        // check fetch schedule
        if (!schedule.shouldFetch(url, crawlDatum, curTime)) {
            LOG.debug("-shouldFetch rejected '{}', fetchTime={}, curTime={}",
                    url, crawlDatum.getFetchTime(), curTime);
            return;
        }

        LongWritable oldGenTime = (LongWritable) ((MapWritable)crawlDatum.getMetaData()).get(
                Nutch.WRITABLE_GENERATE_TIME_KEY);
        if (oldGenTime != null) { // awaiting fetch & update
            if (oldGenTime.get() + genDelay > curTime) // still wait for
                // update
                return;
        }
        float sort = 1.0f;
        try {
            sort = scfilters.generatorSortValue(url, crawlDatum, sort);
        } catch (ScoringFilterException sfe) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Couldn't filter generatorSortValue for {}: {}", url, sfe);
            }
        }

        if (restrictStatus != null
                && !restrictStatus.equalsIgnoreCase(CrawlDatum
                .getStatusName(crawlDatum.getStatus())))
            return;

        // consider only entries with a score superior to the threshold
        if (scoreThreshold != Float.NaN && sort < scoreThreshold)
            return;

        // consider only entries with a retry (or fetch) interval lower than
        // threshold
        if (intervalThreshold != -1
                && crawlDatum.getFetchInterval() > intervalThreshold)
            return;

        // sort by decreasing score, using DecreasingFloatComparator
        sortValue.set(sort);
        // record generation time
        crawlDatum.getMetaData().put(Nutch.WRITABLE_GENERATE_TIME_KEY, genTime);
        entry.datum = crawlDatum;
        entry.url = url;
        context.write(sortValue, entry); // invert for sort by score
    }



}
