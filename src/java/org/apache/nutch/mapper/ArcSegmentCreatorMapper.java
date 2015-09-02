package org.apache.nutch.mapper;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.*;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class ArcSegmentCreatorMapper extends Mapper<Text, BytesWritable, Text, NutchWritable> {
    public static final Logger LOG = LoggerFactory
            .getLogger(ArcSegmentCreatorMapper.class);
    public static final String URL_VERSION = "arc.url.version";
    private Configuration configuration;
    private URLFilters urlFilters;
    private ScoringFilters scfilters;
    private ParseUtil parseUtil;
    private URLNormalizers normalizers;
    private int interval;
    private String segmentName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // set the url filters, scoring filters the parse util and the url
        // normalizers
        this.configuration = context.getConfiguration();
        this.urlFilters = new URLFilters(configuration);
        this.scfilters = new ScoringFilters(configuration);
        this.parseUtil = new ParseUtil(configuration);
        this.normalizers = new URLNormalizers(configuration, URLNormalizers.SCOPE_FETCHER);
        interval = configuration.getInt("db.fetch.interval.default", 2592000);
        segmentName = configuration.get(Nutch.SEGMENT_NAME_KEY);
    }

    @Override
    /**
     * <p>
     * Runs the Map job to translate an arc record into output for Nutch segments.
     * </p>
     *
     * @param key
     *          The arc record header.
     * @param bytes
     *          The arc record raw content bytes.
     * @param output
     *          The output collecter.
     * @param reporter
     *          The progress reporter.
     */
    public void map(Text key, BytesWritable bytes, Context context) throws IOException, InterruptedException {

        String[] headers = key.toString().split("\\s+");
        String urlStr = headers[0];
        String version = headers[2];
        String contentType = headers[3];

        // arcs start with a file description. for now we ignore this as it is not
        // a content record
        if (urlStr.startsWith("filedesc://")) {
            LOG.info("Ignoring file header: " + urlStr);
            return;
        }
        LOG.info("Processing: " + urlStr);

        // get the raw bytes from the arc file, create a new crawldatum
        Text url = new Text();
        CrawlDatum datum = new CrawlDatum(CrawlDatum.STATUS_DB_FETCHED, interval,
                1.0f);

        // normalize and filter the urls
        try {
            urlStr = normalizers.normalize(urlStr, URLNormalizers.SCOPE_FETCHER);
            urlStr = urlFilters.filter(urlStr); // filter the url
        } catch (Exception e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Skipping " + url + ":" + e);
            }
            urlStr = null;
        }

        // if still a good url then process
        if (urlStr != null) {

            url.set(urlStr);
            try {

                // set the protocol status to success and the crawl status to success
                // create the content from the normalized url and the raw bytes from
                // the arc file, TODO: currently this doesn't handle text of errors
                // pages (i.e. 404, etc.). We assume we won't get those.
                ProtocolStatus status = ProtocolStatus.STATUS_SUCCESS;
                Content content = new Content(urlStr, urlStr, bytes.getBytes(),
                        contentType, new Metadata(), configuration);

                // set the url version into the metadata
                content.getMetadata().set(URL_VERSION, version);
                ParseStatus pstatus = null;
                pstatus = output(context, segmentName, url, datum, content, status,
                        CrawlDatum.STATUS_FETCH_SUCCESS);
                context.progress();
            } catch (Throwable t) { // unexpected exception
                logError(url, t);
                output(context, segmentName, url, datum, null, null,
                        CrawlDatum.STATUS_FETCH_RETRY);
            }
        }
    }

    /**
     * <p>
     * Parses the raw content of a single record to create output. This method is
     * almost the same as the {@link org.apache.nutch.Fetcher#output} method in
     * terms of processing and output.
     *
     * @param context
     *          The job context.
     * @param segmentName
     *          The name of the segment to create.
     * @param key
     *          The url of the record.
     * @param datum
     *          The CrawlDatum of the record.
     * @param content
     *          The raw content of the record
     * @param pstatus
     *          The protocol status
     * @param status
     *          The fetch status.
     *
     * @return The result of the parse in a ParseStatus object.
     */
    private ParseStatus output(Context context,
                               String segmentName, Text key, CrawlDatum datum, Content content,
                               ProtocolStatus pstatus, int status) throws InterruptedException {

        // set the fetch status and the fetch time
        datum.setStatus(status);
        datum.setFetchTime(System.currentTimeMillis());
        if (pstatus != null)
            datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY, pstatus);

        ParseResult parseResult = null;
        if (content != null) {
            Metadata metadata = content.getMetadata();
            // add segment to metadata
            metadata.set(Nutch.SEGMENT_NAME_KEY, segmentName);
            // add score to content metadata so that ParseSegment can pick it up.
            try {
                scfilters.passScoreBeforeParsing(key, datum, content);
            } catch (Exception e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Couldn't pass score, url {} ({})", key, e);
                }
            }

            try {

                // parse the content
                parseResult = this.parseUtil.parse(content);
            } catch (Exception e) {
                LOG.warn("Error parsing: {}: {}", key, StringUtils.stringifyException(e));
            }

            // set the content signature
            if (parseResult == null) {
                byte[] signature = SignatureFactory.getSignature(configuration).calculate(
                        content, new ParseStatus().getEmptyParse(configuration));
                datum.setSignature(signature);
            }

            try {
                context.write(key, new NutchWritable(datum));
                context.write(key, new NutchWritable(content));

                if (parseResult != null) {
                    for (Map.Entry<Text, Parse> entry : parseResult) {
                        Text url = entry.getKey();
                        Parse parse = entry.getValue();
                        ParseStatus parseStatus = parse.getData().getStatus();

                        if (!parseStatus.isSuccess()) {
                            LOG.warn("Error parsing: {}: {}", key, parseStatus);
                            parse = parseStatus.getEmptyParse(configuration);
                        }

                        // Calculate page signature.
                        byte[] signature = SignatureFactory.getSignature(configuration)
                                .calculate(content, parse);
                        // Ensure segment name and score are in parseData metadata
                        parse.getData().getContentMeta()
                                .set(Nutch.SEGMENT_NAME_KEY, segmentName);
                        parse.getData().getContentMeta()
                                .set(Nutch.SIGNATURE_KEY, StringUtil.toHexString(signature));
                        // Pass fetch time to content meta
                        parse.getData().getContentMeta()
                                .set(Nutch.FETCH_TIME_KEY, Long.toString(datum.getFetchTime()));
                        if (url.equals(key))
                            datum.setSignature(signature);
                        try {
                            scfilters.passScoreAfterParsing(url, content, parse);
                        } catch (Exception e) {
                            if (LOG.isWarnEnabled()) {
                                LOG.warn("Couldn't pass score, url {} ({})", key, e);
                            }
                        }
                        context.write(url, new NutchWritable(new ParseImpl(new ParseText(
                                parse.getText()), parse.getData(), parse.isCanonical())));
                    }
                }
            } catch (IOException e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error("ArcSegmentCreator caught: {}", StringUtils.stringifyException(e));
                }
            }

            // return parse status if it exits
            if (parseResult != null && !parseResult.isEmpty()) {
                Parse p = parseResult.get(content.getUrl());
                if (p != null) {
                    return p.getData().getStatus();
                }
            }
        }

        return null;
    }

    /**
     * <p>
     * Logs any error that occurs during conversion.
     * </p>
     *
     * @param url
     *          The url we are parsing.
     * @param t
     *          The error that occured.
     */
    private void logError(Text url, Throwable t) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Conversion of {} failed with: {}", url, StringUtils.stringifyException(t));
        }
    }


}
