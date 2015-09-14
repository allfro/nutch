package org.apache.nutch.reducer;


import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilters;
import org.apache.nutch.io.*;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class IndexerReducer extends Reducer<Text, NutchWritable, Text, NutchIndexAction> {
    public static final Logger LOG = LoggerFactory
            .getLogger(IndexerReducer.class);

    public static final String INDEXER_PARAMS = "indexer.additional.params";
    public static final String INDEXER_DELETE = "indexer.delete";
    public static final String INDEXER_DELETE_ROBOTS_NOINDEX = "indexer.delete.robots.noindex";
    public static final String INDEXER_SKIP_NOTMODIFIED = "indexer.skip.notmodified";
    public static final String URL_FILTERING = "indexer.url.filters";
    public static final String URL_NORMALIZING = "indexer.url.normalizers";
    public static final String INDEXER_BINARY_AS_BASE64 = "indexer.binary.base64";

    private boolean skip = false;
    private boolean delete = false;
    private boolean deleteRobotsNoIndex = false;
    private boolean base64 = false;
    private IndexingFilters filters;
    private ScoringFilters scfilters;

    // using normalizers and/or filters
    private boolean normalize = false;
    private boolean filter = false;


    // url normalizers, filters and job configuration
    private URLNormalizers urlNormalizers;
    private URLFilters urlFilters;

    /** Predefined action to delete documents from the index */
    private static final NutchIndexAction DELETE_ACTION = new NutchIndexAction(
            null, NutchIndexAction.DELETE);


    private Counter deletedNoIndex;
    private Counter deletedGone;
    private Counter deletedRedirects;
    private Counter deletedDuplicates;
    private Counter deletedSkipped;
    private Counter errors;
    private Counter filterSkipped;
    private Counter scoringError;
    private Counter indexed;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        this.filters = new IndexingFilters(configuration);
        this.scfilters = new ScoringFilters(configuration);
        this.delete = configuration.getBoolean(INDEXER_DELETE, false);
        this.deleteRobotsNoIndex = configuration.getBoolean(INDEXER_DELETE_ROBOTS_NOINDEX,
                false);
        this.skip = configuration.getBoolean(INDEXER_SKIP_NOTMODIFIED, false);
        this.base64 = configuration.getBoolean(INDEXER_BINARY_AS_BASE64, false);

        normalize = configuration.getBoolean(URL_NORMALIZING, false);
        filter = configuration.getBoolean(URL_FILTERING, false);

        if (normalize) {
            urlNormalizers = new URLNormalizers(configuration, URLNormalizers.SCOPE_INDEXER);
        }

        if (filter) {
            urlFilters = new URLFilters(configuration);
        }

        deletedNoIndex = context.getCounter("IndexerStatus", "deleted (robots=noindex)");
        deletedGone = context.getCounter("IndexerStatus", "deleted (gone)");
        deletedRedirects = context.getCounter("IndexerStatus", "deleted redirects");
        deletedDuplicates = context.getCounter("IndexerStatus", "deleted duplicates");
        deletedSkipped = context.getCounter("IndexerStatus", "skipped (not modified)");
        errors = context.getCounter("IndexerStatus", "errors (IndexingFilter)");
        filterSkipped = context.getCounter("IndexerStatus", "skipped by indexing filters");
        scoringError = context.getCounter("IndexerStatus", "errors (ScoringFilter)");
        indexed = context.getCounter("IndexerStatus", "indexed (add/update)");
    }

    @Override
    protected void reduce(Text key, Iterable<NutchWritable> values, Context context)
            throws IOException, InterruptedException {
        Inlinks inlinks = null;
        CrawlDatum dbDatum = null;
        CrawlDatum fetchDatum = null;
        Content content = null;
        ParseData parseData = null;
        ParseText parseText = null;

        for (Writable value: values) {
            value = ((NutchWritable)value).get();
            if (value instanceof Inlinks) {
                inlinks = (Inlinks) value;
            } else if (value instanceof CrawlDatum) {
                final CrawlDatum datum = (CrawlDatum) value;
                if (CrawlDatum.hasDbStatus(datum)) {
                    dbDatum = datum;
                } else if (CrawlDatum.hasFetchStatus(datum)) {
                    // don't index unmodified (empty) pages
                    if (datum.getStatus() != CrawlDatum.STATUS_FETCH_NOTMODIFIED) {
                        fetchDatum = datum;
                    }
                } else if (CrawlDatum.STATUS_LINKED == datum.getStatus()
                        || CrawlDatum.STATUS_SIGNATURE == datum.getStatus()
                        || CrawlDatum.STATUS_PARSE_META == datum.getStatus()) {
                } else {
                    throw new RuntimeException("Unexpected status: " + datum.getStatus());
                }
            } else if (value instanceof ParseData) {
                parseData = (ParseData) value;

                // Handle robots meta? https://issues.apache.org/jira/browse/NUTCH-1434
                if (deleteRobotsNoIndex) {
                    // Get the robots meta data
                    String robotsMeta = parseData.getMeta("robots");

                    // Has it a noindex for this url?
                    if (robotsMeta != null
                            && robotsMeta.toLowerCase().contains("noindex")) {
                        // Delete it!
                        context.write(key, DELETE_ACTION);
                        deletedNoIndex.increment(1);
                        return;
                    }
                }
            } else if (value instanceof ParseText) {
                parseText = (ParseText) value;
            } else if (value instanceof Content) {
                content = (Content)value;
            } else if (LOG.isWarnEnabled()) {
                LOG.warn("Unrecognized type: {}", value.getClass());
            }
        }

        // Whether to delete GONE or REDIRECTS
        if (delete && fetchDatum != null && dbDatum != null) {
            if (fetchDatum.getStatus() == CrawlDatum.STATUS_FETCH_GONE
                    || dbDatum.getStatus() == CrawlDatum.STATUS_DB_GONE) {
                deletedGone.increment(1);
                context.write(key, DELETE_ACTION);
                return;
            }

            if (fetchDatum.getStatus() == CrawlDatum.STATUS_FETCH_REDIR_PERM
                    || fetchDatum.getStatus() == CrawlDatum.STATUS_FETCH_REDIR_TEMP
                    || dbDatum.getStatus() == CrawlDatum.STATUS_DB_REDIR_PERM
                    || dbDatum.getStatus() == CrawlDatum.STATUS_DB_REDIR_TEMP) {
                deletedRedirects.increment(1);
                context.write(key, DELETE_ACTION);
                return;
            }
        }

        if (fetchDatum == null || dbDatum == null || parseText == null
                || parseData == null) {
            return; // only have inlinks
        }

        // Whether to delete pages marked as duplicates
        if (delete && dbDatum.getStatus() == CrawlDatum.STATUS_DB_DUPLICATE) {
            deletedDuplicates.increment(1);
            context.write(key, DELETE_ACTION);
            return;
        }

        // Whether to skip DB_NOTMODIFIED pages
        if (skip && dbDatum.getStatus() == CrawlDatum.STATUS_DB_NOTMODIFIED) {
            deletedSkipped.increment(1);
            return;
        }

        if (!parseData.getStatus().isSuccess()
                || fetchDatum.getStatus() != CrawlDatum.STATUS_FETCH_SUCCESS) {
            return;
        }

        NutchDocument doc = new NutchDocument();
        doc.add("id", key.toString());

        final Metadata metadata = parseData.getContentMeta();

        // add segment, used to map from merged index back to segment files
        doc.add("segment", metadata.get(Nutch.SEGMENT_NAME_KEY));

        // add digest, used by dedup
        doc.add("digest", metadata.get(Nutch.SIGNATURE_KEY));

        final Parse parse = new ParseImpl(parseText, parseData);
        try {
            // extract information from dbDatum and pass it to
            // fetchDatum so that indexing filters can use it
            final Text url = (Text) dbDatum.getMetaData().get(
                    Nutch.WRITABLE_REPR_URL_KEY);
            if (url != null) {
                // Representation URL also needs normalization and filtering.
                // If repr URL is excluded by filters we still accept this document
                // but represented by its primary URL ("key") which has passed URL
                // filters.
                String urlString = filterUrl(normalizeUrl(url.toString()));
                if (urlString != null) {
                    url.set(urlString);
                    fetchDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY, url);
                }
            }
            // run indexing filters
            doc = this.filters.filter(doc, parse, key, fetchDatum, inlinks);
        } catch (final IndexingException e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Error indexing {}: {}", key, e);
            }
            errors.increment(1);
            return;
        }

        // skip documents discarded by indexing filters
        if (doc == null) {
            filterSkipped.increment(1);
            return;
        }

        float boost = 1.0f;
        // run scoring filters
        try {
            boost = this.scfilters.indexerScore(key, doc, dbDatum, fetchDatum, parse,
                    inlinks, boost);
        } catch (final ScoringFilterException e) {
            scoringError.increment(1);
            if (LOG.isWarnEnabled()) {
                LOG.warn("Error calculating score {}: {}", key, e);
            }
            return;
        }
        // apply boost to all indexed fields.
        doc.setWeight(boost);
        // store boost for use by explain and dedup
        doc.add("boost", Float.toString(boost));

        if (content != null) {
            // Get the original unencoded content
            String binary = new String(content.getContent());

            // optionally encode as base64
            if (base64) {
                binary = Base64.encodeBase64String(StringUtils.getBytesUtf8(binary));
            }

            doc.add("binaryContent", binary);
        }

        indexed.increment(1);

        NutchIndexAction action = new NutchIndexAction(doc, NutchIndexAction.ADD);
        context.write(key, action);
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
