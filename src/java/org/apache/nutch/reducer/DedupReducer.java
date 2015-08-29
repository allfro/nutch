package org.apache.nutch.reducer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.DeduplicationJob;

import java.io.IOException;

/**
 * Created by ndouba on 15-08-28.
 */
public class DedupReducer extends
        Reducer<BytesWritable, CrawlDatum, Text, CrawlDatum> {

    Counter dedupJobStatus;

    private void writeOutAsDuplicate(CrawlDatum datum, Context context)
            throws IOException, InterruptedException {
        datum.setStatus(CrawlDatum.STATUS_DB_DUPLICATE);
        Text key = (Text) datum.getMetaData().remove(DeduplicationJob.URL_KEY);
        dedupJobStatus.increment(1);
        context.write(key, datum);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        dedupJobStatus = context.getCounter("DeduplicationJobStatus", "Documents marked as duplicate");
    }

    @Override
    protected void reduce(BytesWritable key, Iterable<CrawlDatum> values, Context context) throws IOException, InterruptedException {
        CrawlDatum existingDoc = null;

        for (CrawlDatum newDoc : values) {
            if (existingDoc == null) {
                existingDoc = new CrawlDatum();
                existingDoc.set(newDoc);
                continue;
            }
            // compare based on score
            if (existingDoc.getScore() < newDoc.getScore()) {
                writeOutAsDuplicate(existingDoc, context);
                existingDoc = new CrawlDatum();
                existingDoc.set(newDoc);
                continue;
            } else if (existingDoc.getScore() > newDoc.getScore()) {
                // mark new one as duplicate
                writeOutAsDuplicate(newDoc, context);
                continue;
            }
            // same score? delete the one which is oldest
            if (existingDoc.getFetchTime() > newDoc.getFetchTime()) {
                // mark new one as duplicate
                writeOutAsDuplicate(newDoc, context);
                continue;
            } else if (existingDoc.getFetchTime() < newDoc.getFetchTime()) {
                // mark existing one as duplicate
                writeOutAsDuplicate(existingDoc, context);
                existingDoc = new CrawlDatum();
                existingDoc.set(newDoc);
                continue;
            }
            // same time? keep the one which has the shortest URL
            String urlExisting = existingDoc.getMetaData().get(DeduplicationJob.URL_KEY).toString();
            String urlnewDoc = newDoc.getMetaData().get(DeduplicationJob.URL_KEY).toString();
            if (urlExisting.length() < urlnewDoc.length()) {
                // mark new one as duplicate
                writeOutAsDuplicate(newDoc, context);
            } else if (urlExisting.length() > urlnewDoc.length()) {
                // mark existing one as duplicate
                writeOutAsDuplicate(existingDoc, context);
                existingDoc = new CrawlDatum();
                existingDoc.set(newDoc);
            }
        }
    }

}
