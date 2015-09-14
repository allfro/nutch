package org.apache.nutch.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.io.Inlink;
import org.apache.nutch.io.Inlinks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by ndouba on 9/2/15.
 */
public class LinkDbMergerReducer extends Reducer<Text, Inlinks, Text, Inlinks> {
    private static final Logger LOG = LoggerFactory.getLogger(LinkDbMergerReducer.class);

    private int maxInlinks;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        maxInlinks = context.getConfiguration().getInt("db.max.inlinks", 10000);
    }

    @Override
    protected void reduce(Text key, Iterable<Inlinks> values, Context context) throws IOException, InterruptedException {
        Inlinks result = new Inlinks();

        for (Inlinks inlinks: values) {
            int end = Math.min(maxInlinks - result.size(), inlinks.size());
            Iterator<Inlink> it = inlinks.iterator();
            int i = 0;
            while (it.hasNext() && i++ < end) {
                result.add(it.next());
            }
        }
        if (result.size() == 0)
            return;
        context.write(key, result);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
