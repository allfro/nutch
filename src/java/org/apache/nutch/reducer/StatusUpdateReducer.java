package org.apache.nutch.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.crawl.CrawlDatum;

import java.io.IOException;
import java.util.Iterator;

/** Combine multiple new entries for a url. */
public class StatusUpdateReducer extends
        Reducer<Text, CrawlDatum, Text, CrawlDatum> {

    private CrawlDatum old = new CrawlDatum();
    private CrawlDatum duplicate = new CrawlDatum();

    @Override
    protected void reduce(Text key, Iterable<CrawlDatum> values, Context context) throws IOException, InterruptedException {
        boolean duplicateSet = false;


        for (CrawlDatum val: values) {
            if (val.getStatus() == CrawlDatum.STATUS_DB_DUPLICATE) {
                duplicate.set(val);
                duplicateSet = true;
            } else {
                old.set(val);
            }
        }

        // keep the duplicate if there is one
        if (duplicateSet) {
            context.write(key, duplicate);
            return;
        }

        // no duplicate? keep old one then
        context.write(key, old);
    }
}
