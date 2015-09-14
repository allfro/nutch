package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.io.*;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class ParseSegmentMapper extends Mapper<WritableComparable<?>, Content, Text, ParseImpl> {


    public static final Logger LOG = LoggerFactory.getLogger(ParseSegmentMapper.class);

    public static final String SKIP_TRUNCATED = "parser.skip.truncated";

    private ScoringFilters scfilters;

    private ParseUtil parseUtil;

    private boolean skipTruncated;
    private Configuration configuration;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.configuration = context.getConfiguration();
        this.scfilters = new ScoringFilters(configuration);
        skipTruncated = configuration.getBoolean(SKIP_TRUNCATED, true);
    }

    @Override
    protected void map(WritableComparable<?> key, Content content, Context context)
            throws IOException, InterruptedException {
        // convert on the fly from old UTF8 keys
        if (key instanceof Text) {
            key = new Text(key.toString());
        }

        int status = Integer.parseInt(content.getMetadata().get(
                Nutch.FETCH_STATUS_KEY));
        if (status != CrawlDatum.STATUS_FETCH_SUCCESS) {
            // content not fetched successfully, skip document
            LOG.debug("Skipping {} as content is not fetched successfully", key);
            return;
        }

        if (skipTruncated && isTruncated(content)) {
            return;
        }

        ParseResult parseResult = null;
        try {
            if (parseUtil == null)
                parseUtil = new ParseUtil(configuration);
            parseResult = parseUtil.parse(content);
        } catch (Exception e) {
            LOG.warn("Error parsing: {}: {}", key, StringUtils.stringifyException(e));
            return;
        }

        for (Map.Entry<Text, Parse> entry : parseResult) {
            Text url = entry.getKey();
            Parse parse = entry.getValue();
            ParseStatus parseStatus = parse.getData().getStatus();

            long start = System.currentTimeMillis();

            context.getCounter("ParserStatus",
                    ParseStatus.majorCodes[parseStatus.getMajorCode()]).increment(1);

            if (!parseStatus.isSuccess()) {
                LOG.warn("Error parsing: {}: {}", key, parseStatus);
                parse = parseStatus.getEmptyParse(configuration);
            }

            // pass segment name to parse data
            parse.getData().getContentMeta()
                    .set(Nutch.SEGMENT_NAME_KEY, configuration.get(Nutch.SEGMENT_NAME_KEY));

            // compute the new signature
            byte[] signature = SignatureFactory.getSignature(configuration).calculate(
                    content, parse);
            parse.getData().getContentMeta()
                    .set(Nutch.SIGNATURE_KEY, StringUtil.toHexString(signature));

            try {
                scfilters.passScoreAfterParsing(url, content, parse);
            } catch (ScoringFilterException e) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Error passing score: {}: {}", url, e.getMessage());
                }
            }

            long end = System.currentTimeMillis();
            LOG.info("Parsed ({}ms): {}", end - start, url);

            context.write(
                    url,
                    new ParseImpl(new ParseText(parse.getText()), parse.getData(), parse
                            .isCanonical()));
        }
    }

    /**
     * Checks if the page's content is truncated.
     *
     * @param content
     * @return If the page is truncated <code>true</code>. When it is not, or when
     *         it could be determined, <code>false</code>.
     */
    public static boolean isTruncated(Content content) {
        byte[] contentBytes = content.getContent();
        if (contentBytes == null)
            return false;
        Metadata metadata = content.getMetadata();
        if (metadata == null)
            return false;

        String lengthStr = metadata.get(Response.CONTENT_LENGTH);
        if (lengthStr != null)
            lengthStr = lengthStr.trim();
        if (StringUtil.isEmpty(lengthStr)) {
            return false;
        }
        int inHeaderSize;
        String url = content.getUrl();
        try {
            inHeaderSize = Integer.parseInt(lengthStr);
        } catch (NumberFormatException e) {
            LOG.warn("Wrong contentlength format for " + url, e);
            return false;
        }
        int actualSize = contentBytes.length;
        if (inHeaderSize > actualSize) {
            LOG.info(url + " skipped. Content of size " + inHeaderSize
                    + " was truncated to " + actualSize);
            return true;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(url + " actualSize=" + actualSize + " inHeaderSize="
                    + inHeaderSize);
        }
        return false;
    }

}
