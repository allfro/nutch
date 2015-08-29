package org.apache.nutch.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.crawl.CrawlDatum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Combine multiple new entries for a url. */
public class InjectorReducer extends
        Reducer<Text, CrawlDatum, Text, CrawlDatum> {
    private int interval;
    private float scoreInjected;
    private boolean overwrite = false;
    private boolean update = false;
    private static final Logger LOG = LoggerFactory.getLogger(InjectorReducer.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        interval = configuration.getInt("db.fetch.interval.default", 2592000);
        scoreInjected = configuration.getFloat("db.score.injected", 1.0f);
        overwrite = configuration.getBoolean("db.injector.overwrite", false);
        update = configuration.getBoolean("db.injector.update", false);
        LOG.info("Injector: overwrite: " + overwrite);
        LOG.info("Injector: update: " + update);
    }

    @Override
    protected void reduce(Text key, Iterable<CrawlDatum> values, Context context)
            throws IOException, InterruptedException {

        CrawlDatum old = new CrawlDatum();
        CrawlDatum injected = new CrawlDatum();

        boolean oldSet = false;
        boolean injectedSet = false;

        for (CrawlDatum val: values) {
            if (val.getStatus() == CrawlDatum.STATUS_INJECTED) {
                injected.set(val);
                injected.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
                injectedSet = true;
            } else {
                old.set(val);
                oldSet = true;
            }
        }

        CrawlDatum res;

        // Old default behaviour
        if (injectedSet && !oldSet) {
            res = injected;
        } else {
            res = old;
        }

        if (injectedSet && oldSet) {
            context.getCounter("injector", "urls_merged").increment(1);
        }

        /**
         * Whether to overwrite, ignore or update existing records
         *
         * @see https://issues.apache.org/jira/browse/NUTCH-1405
         */
        // Injected record already exists and update but not overwrite
        if (injectedSet && oldSet && update && !overwrite) {
            res = old;
            old.putAllMetaData(injected);
            old.setScore(injected.getScore() != scoreInjected ? injected.getScore()
                    : old.getScore());
            old.setFetchInterval(injected.getFetchInterval() != interval ? injected
                    .getFetchInterval() : old.getFetchInterval());
        }

        // Injected record already exists and overwrite
        if (injectedSet && oldSet && overwrite) {
            res = injected;
        }

        context.write(key, res);
    }
}
