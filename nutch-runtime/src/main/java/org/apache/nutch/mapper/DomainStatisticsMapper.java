package org.apache.nutch.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.util.URLUtil;
import org.apache.nutch.util.domain.DomainStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;

/**
 * Created by ndouba on 9/2/15.
 */
public class DomainStatisticsMapper extends
        Mapper<Text, CrawlDatum, Text, LongWritable> {
    private static final Logger LOG = LoggerFactory.getLogger(DomainStatisticsMapper.class);

    int mode = 0;
    private static final Text FETCHED_TEXT = new Text("FETCHED");
    private static final Text NOT_FETCHED_TEXT = new Text("NOT_FETCHED");
    private Counter fetched;
    private Counter notFetched;
    private Counter emptyResult;

    public enum MyCounter {
        FETCHED, NOT_FETCHED, EMPTY_RESULT
    };

    @Override
    public void setup(Context context) {
        mode = context.getConfiguration().getInt("domain.statistics.mode",
                DomainStatistics.MODE_DOMAIN);
        fetched = context.getCounter(MyCounter.FETCHED);
        notFetched = context.getCounter(MyCounter.NOT_FETCHED);
        emptyResult = context.getCounter(MyCounter.EMPTY_RESULT);
    }

    @Override
    public void map(Text urlText, CrawlDatum datum, Context context)
            throws IOException, InterruptedException {

        if (datum.getStatus() == CrawlDatum.STATUS_DB_FETCHED
                || datum.getStatus() == CrawlDatum.STATUS_DB_NOTMODIFIED) {

            try {
                URL url = new URL(urlText.toString());
                String out = null;
                switch (mode) {
                    case DomainStatistics.MODE_HOST:
                        out = url.getHost();
                        break;
                    case DomainStatistics.MODE_DOMAIN:
                        out = URLUtil.getDomainName(url);
                        break;
                    case DomainStatistics.MODE_SUFFIX:
                        out = URLUtil.getDomainSuffix(url).getDomain();
                        break;
                    case DomainStatistics.MODE_TLD:
                        out = URLUtil.getTopLevelDomainName(url);
                        break;
                }
                if (out != null && out.trim().isEmpty()) {
                    LOG.info("url : {}", url);
                    emptyResult.increment(1);
                    return;
                }

                context.write(new Text(out), new LongWritable(1));
            } catch (Exception ignored) {
            }

            fetched.increment(1);
            context.write(FETCHED_TEXT, new LongWritable(1));
        } else {
            notFetched.increment(1);
            context.write(NOT_FETCHED_TEXT, new LongWritable(1));
        }
    }
}
