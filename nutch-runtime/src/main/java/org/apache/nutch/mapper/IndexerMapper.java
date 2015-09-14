package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.NutchWritable;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class IndexerMapper extends Mapper<Text, Writable, Text, NutchWritable> {

    public static final Logger LOG = LoggerFactory
            .getLogger(IndexerMapper.class);

    public static final String URL_FILTERING = "indexer.url.filters";
    public static final String URL_NORMALIZING = "indexer.url.normalizers";

    // using normalizers and/or filters
    private boolean normalize = false;
    private boolean filter = false;

    // url normalizers, filters and job configuration
    private URLNormalizers urlNormalizers;
    private URLFilters urlFilters;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        normalize = configuration.getBoolean(URL_NORMALIZING, false);
        filter = configuration.getBoolean(URL_FILTERING, false);

        if (normalize) {
            urlNormalizers = new URLNormalizers(configuration, URLNormalizers.SCOPE_INDEXER);
        }

        if (filter) {
            urlFilters = new URLFilters(configuration);
        }
    }


    @Override
    protected void map(Text key, Writable value, Context context) throws IOException, InterruptedException {
        String urlString = filterUrl(normalizeUrl(key.toString()));

        if (urlString == null) {
            return;
        } else {
            key.set(urlString);
        }

        context.write(key, new NutchWritable(value));
    }

    /**
     * Filters the given url.
     *
     * @param url
     *          The url to filter.
     *
     * @return The filtered url or null.
     */
    private String filterUrl(String url) {
        if (!filter) {
            return url;
        }

        try {
            url = urlFilters.filter(url);
        } catch (Exception e) {
            url = null;
        }

        return url;
    }

    /**
     * Normalizes and trims extra whitespace from the given url.
     *
     * @param url
     *          The url to normalize.
     *
     * @return The normalized url.
     */
    private String normalizeUrl(String url) {
        if (!normalize) {
            return url;
        }

        String normalized = null;
        if (urlNormalizers != null) {
            try {
                // normalize and trim the url
                normalized = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INDEXER);
            } catch (Exception e) {
                LOG.warn("Skipping {}:", url, e);
            }
        }
        return (normalized != null)?normalized.trim():null;
    }
}
