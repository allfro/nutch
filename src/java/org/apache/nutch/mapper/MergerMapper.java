package org.apache.nutch.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.crawl.MapWritable;

import java.io.IOException;
import java.util.Map;

/**
 * Created by ndouba on 15-08-28.
 */
public class MergerMapper extends Reducer<Text, CrawlDatum, Text, CrawlDatum> {
    private MapWritable meta;
    private CrawlDatum res = new CrawlDatum();
    private FetchSchedule schedule;

    public void close() throws IOException {
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        schedule = FetchScheduleFactory.getFetchSchedule(context.getConfiguration());
    }

    @Override
    protected void reduce(Text key, Iterable<CrawlDatum> values, Context context) throws IOException, InterruptedException {
        long resTime = 0L;
        boolean resSet = false;
        meta = new MapWritable();
        for (CrawlDatum val : values) {
            if (!resSet) {
                res.set(val);
                resSet = true;
                resTime = schedule.calculateLastFetchTime(res);
                for (Map.Entry<Writable, Writable> e : res.getMetaData().entrySet()) {
                    meta.put(e.getKey(), e.getValue());
                }
                continue;
            }
            // compute last fetch time, and pick the latest
            long valTime = schedule.calculateLastFetchTime(val);
            if (valTime > resTime) {
                // collect all metadata, newer values override older values
                for (Map.Entry<Writable, Writable> e : val.getMetaData().entrySet()) {
                    meta.put(e.getKey(), e.getValue());
                }
                res.set(val);
                resTime = valTime;
            } else {
                // insert older metadata before newer
                for (Map.Entry<Writable, Writable> e : meta.entrySet()) {
                    val.getMetaData().put(e.getKey(), e.getValue());
                }
                meta = val.getMetaData();
            }
        }
        res.setMetaData(meta);
        context.write(key, res);
    }
}
