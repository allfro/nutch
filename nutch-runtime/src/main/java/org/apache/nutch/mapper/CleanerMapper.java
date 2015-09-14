package org.apache.nutch.mapper;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.CrawlDatum;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class CleanerMapper extends
        Mapper<Text, CrawlDatum, ByteWritable, Text> {

    private ByteWritable OUT = new ByteWritable(CrawlDatum.STATUS_DB_GONE);

    @Override
    protected void map(Text key, CrawlDatum value, Context context) throws IOException, InterruptedException {
        if (value.getStatus() == CrawlDatum.STATUS_DB_GONE
                || value.getStatus() == CrawlDatum.STATUS_DB_DUPLICATE)
            context.write(OUT, key);
    }

}
