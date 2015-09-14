package org.apache.nutch.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.io.Content;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.io.NutchWritable;
import org.apache.nutch.io.ParseText;
import org.apache.nutch.parse.ParseData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class SegmentReaderReducer extends Reducer<Text, NutchWritable, Text, Text> {

    long recNo = 0L;

    public static final Logger LOG = LoggerFactory.getLogger(SegmentReaderReducer.class);

    @Override
    public void reduce(Text key, Iterable<NutchWritable> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder dump = new StringBuilder();

        dump.append("\nRecno:: ").append(recNo++).append("\n");
        dump.append("URL:: ").append(key.toString()).append("\n");
        for (Writable value: values) {
            value = ((NutchWritable)values).get(); // unwrap
            if (value instanceof CrawlDatum) {
                dump.append("\nCrawlDatum::\n").append(value.toString());
            } else if (value instanceof Content) {
                dump.append("\nContent::\n").append(value.toString());
            } else if (value instanceof ParseData) {
                dump.append("\nParseData::\n").append(value.toString());
            } else if (value instanceof ParseText) {
                dump.append("\nParseText::\n").append(value.toString());
            } else if (LOG.isWarnEnabled()) {
                LOG.warn("Unrecognized type: {}", value.getClass());
            }
        }
        context.write(key, new Text(dump.toString()));
    }

}
