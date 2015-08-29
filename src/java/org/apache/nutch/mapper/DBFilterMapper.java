package org.apache.nutch.mapper;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.DeduplicationJob;

import java.io.IOException;

/**
 * Created by ndouba on 15-08-28.
 */
public class DBFilterMapper extends
        Mapper<Text, CrawlDatum, BytesWritable, CrawlDatum> {

    @Override
    protected void map(Text key, CrawlDatum value, Context context) throws IOException, InterruptedException {

        if (value.getStatus() == CrawlDatum.STATUS_DB_FETCHED
                || value.getStatus() == CrawlDatum.STATUS_DB_NOTMODIFIED) {
            // || value.getStatus() ==CrawlDatum.STATUS_DB_GONE){
            byte[] signature = value.getSignature();
            if (signature == null)
                return;
            BytesWritable sig = new BytesWritable(signature);
            // add the URL as a temporary MD
            value.getMetaData().put(DeduplicationJob.URL_KEY, key);
            // reduce on the signature
            context.write(sig, value);
        }
    }
}
