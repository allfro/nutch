package org.apache.nutch.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.indexer.IndexWriters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class DeleterReducer extends
        Reducer<ByteWritable, Text, Text, ByteWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(DeleterReducer.class);
    private int totalDeleted = 0;

    private boolean noCommit = false;

    IndexWriters writers = null;
    private Counter deleted;

    @Override
    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();

        writers = new IndexWriters(configuration);
        try {
            writers.open(configuration, "Deletion");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        noCommit = configuration.getBoolean("noCommit", false);
        deleted = context.getCounter("CleaningJobStatus", "Deleted documents");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // BUFFERING OF CALLS TO INDEXER SHOULD BE HANDLED AT INDEXER LEVEL
        // if (numDeletes > 0) {
        // LOG.info("CleaningJob: deleting " + numDeletes + " documents");
        // // TODO updateRequest.process(solr);
        // totalDeleted += numDeletes;
        // }

        writers.close();

        if (totalDeleted > 0 && !noCommit) {
            writers.commit();
        }

        LOG.info("CleaningJob: deleted a total of {} documents", totalDeleted);
    }

    @Override
    public void reduce(ByteWritable key, Iterable<Text> values, Context context) throws IOException {
        for (Text document: values) {
            writers.delete(document.toString());
            totalDeleted++;
            deleted.increment(1);
            // if (numDeletes >= NUM_MAX_DELETE_REQUEST) {
            // LOG.info("CleaningJob: deleting " + numDeletes
            // + " documents");
            // // TODO updateRequest.process(solr);
            // // TODO updateRequest = new UpdateRequest();
            // writers.delete(key.toString());
            // totalDeleted += numDeletes;
            // numDeletes = 0;
            // }
        }
    }
}
