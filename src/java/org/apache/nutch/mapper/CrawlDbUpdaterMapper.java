package org.apache.nutch.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.crawl.CrawlDatum;

import java.io.IOException;

/**
 * Update the CrawlDB so that the next generate won't include the same URLs.
 */
public class CrawlDbUpdaterMapper extends
        Mapper<Text, CrawlDatum, Text, CrawlDatum> {
    @Override
    protected void map(Text key, CrawlDatum value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }

}
