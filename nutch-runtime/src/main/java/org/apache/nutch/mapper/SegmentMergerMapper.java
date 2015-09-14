package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.metadata.MetaWrapper;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class SegmentMergerMapper extends Mapper<Text, MetaWrapper, Text, MetaWrapper> {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentMergerMapper.class);
    private Text newKey = new Text();
    private URLNormalizers normalizers = null;
    private URLFilters filters = null;

    @Override
    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
        if (configuration.getBoolean("segment.merger.filter", false)) {
            filters = new URLFilters(configuration);
        }
        if (configuration.getBoolean("segment.merger.normalizer", false))
            normalizers = new URLNormalizers(configuration, URLNormalizers.SCOPE_DEFAULT);
    }

    @Override
    public void map(Text key, MetaWrapper value, Context context)
            throws IOException, InterruptedException {
        String url = key.toString();
        if (normalizers != null) {
            try {
                url = normalizers.normalize(url, URLNormalizers.SCOPE_DEFAULT); // normalize
                // the
                // url
            } catch (Exception e) {
                LOG.warn("Skipping " + url + ":" + e.getMessage());
                url = null;
            }
        }
        if (url != null && filters != null) {
            try {
                url = filters.filter(url);
            } catch (Exception e) {
                LOG.warn("Skipping key " + url + ": " + e.getMessage());
                url = null;
            }
        }
        if (url != null) {
            newKey.set(url);
            context.write(newKey, value);
        }
    }
}
