/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.fetcher;

import crawlercommons.robots.BaseRobotRules;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.mapper.ParseSegmentMapper;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.service.NutchServer;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class picks items from queues and fetches the pages.
 */
public class FetcherThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(FetcherThread.class);
    private final Counter robotsDenied;
    private final MapContext<Text, CrawlDatum, Text, NutchWritable> context;

    private Configuration configuration;
    private URLFilters urlFilters;
    private ScoringFilters scfilters;
    private ParseUtil parseUtil;
    private URLNormalizers normalizers;
    private ProtocolFactory protocolFactory;
    private long maxCrawlDelay;
    private String queueMode;
    private int maxRedirect;
    private String reprUrl;
    private boolean redirecting;
    private int redirectCount;
    private boolean ignoreExternalLinks;

    private final int maxOutlinks;
    private final int interval;
    private int maxOutlinkDepth;
    private int maxOutlinkDepthNumLinks;
    private boolean outlinksIgnoreExternal;

    private int outlinksDepthDivisor;
    private boolean skipTruncated;

    private boolean halted = false;

    private AtomicInteger activeThreads;

    private FetchItemQueues fetchQueues;

    private QueueFeeder feeder;

    private AtomicInteger spinWaiting;

    private AtomicLong lastRequestStart;

    private AtomicInteger errors;

    private String segmentName;

    private boolean parsing;

    private boolean storingContent;

    private AtomicInteger pages;

    private AtomicLong bytes;

    //Used by the REST service
    private FetchNode fetchNode;
    private boolean reportToNutchServer;
    private Counter robotsDeniedMaxCrawlDelay;
    private Counter aboveExceptionThresholdInQueue;
    private Counter notCreatedRedirect;
    private Counter outlinksFollowing;
    private Counter outlinksDetected;

    public FetcherThread(MapContext<Text, CrawlDatum, Text, NutchWritable> context, AtomicInteger activeThreads, FetchItemQueues fetchQueues,
                         QueueFeeder feeder, AtomicInteger spinWaiting, AtomicLong lastRequestStart,
                         AtomicInteger errors, String segmentName, boolean parsing,
                         boolean storingContent, AtomicInteger pages, AtomicLong bytes) {
        this.context = context;
        this.configuration = context.getConfiguration();
        this.setDaemon(true); // don't hang JVM on exit
        this.setName("FetcherThread"); // use an informative name
        this.urlFilters = new URLFilters(configuration);
        this.scfilters = new ScoringFilters(configuration);
        this.parseUtil = new ParseUtil(configuration);
        this.skipTruncated = configuration.getBoolean(ParseSegment.SKIP_TRUNCATED, true);
        this.protocolFactory = new ProtocolFactory(configuration);
        this.normalizers = new URLNormalizers(configuration, URLNormalizers.SCOPE_FETCHER);
        this.maxCrawlDelay = configuration.getInt("fetcher.max.crawl.delay", 30) * 1000;
        this.activeThreads = activeThreads;
        this.fetchQueues = fetchQueues;
        this.feeder = feeder;
        this.spinWaiting = spinWaiting;
        this.lastRequestStart = lastRequestStart;
        this.errors = errors;
        this.segmentName = segmentName;
        this.parsing = parsing;
        this.storingContent = storingContent;
        this.pages = pages;
        this.bytes = bytes;
        queueMode = configuration.get("fetcher.queue.mode", FetchItemQueues.QUEUE_MODE_HOST);
        // check that the mode is known
        if (!queueMode.equals(FetchItemQueues.QUEUE_MODE_IP)
                && !queueMode.equals(FetchItemQueues.QUEUE_MODE_DOMAIN)
                && !queueMode.equals(FetchItemQueues.QUEUE_MODE_HOST)) {
            LOG.error("Unknown partition mode : {} - forcing to byHost", queueMode);
            queueMode = FetchItemQueues.QUEUE_MODE_HOST;
        }
        LOG.info("Using queue mode : {}", queueMode);
        this.maxRedirect = configuration.getInt("http.redirect.max", 3);
        this.ignoreExternalLinks = configuration.getBoolean("db.ignore.external.links", false);

        int maxOutlinksPerPage = configuration.getInt("db.max.outlinks.per.page", 100);
        maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE
                : maxOutlinksPerPage;
        interval = configuration.getInt("db.fetch.interval.default", 2592000);
        ignoreExternalLinks = configuration.getBoolean("db.ignore.external.links", false);
        maxOutlinkDepth = configuration.getInt("fetcher.follow.outlinks.depth", -1);
        outlinksIgnoreExternal = configuration.getBoolean(
                "fetcher.follow.outlinks.ignore.external", false);
        maxOutlinkDepthNumLinks = configuration.getInt(
                "fetcher.follow.outlinks.num.links", 4);
        outlinksDepthDivisor = configuration.getInt(
                "fetcher.follow.outlinks.depth.divisor", 2);
        robotsDenied = context.getCounter("FetcherStatus", "robots_denied");
        robotsDeniedMaxCrawlDelay = context.getCounter("FetcherStatus", "robots_denied_maxcrawldelay");
        aboveExceptionThresholdInQueue = context.getCounter("FetcherStatus", "AboveExceptionThresholdInQueue");
        notCreatedRedirect = context.getCounter("FetcherStatus", "FetchItem.notCreated.redirect");
        outlinksFollowing = context.getCounter("FetcherOutlinks", "outlinks_following");
        outlinksDetected = context.getCounter("FetcherOutlinks", "outlinks_detected");
    }

    @SuppressWarnings("fallthrough")
    public void run() {
        activeThreads.incrementAndGet(); // count threads

        FetchItem fit = null;
        try {
            // checking for the server to be running and fetcher.parse to be true
            if (parsing && NutchServer.getInstance().isRunning())
                reportToNutchServer = true;

            while (true) {
                // creating FetchNode for storing in FetchNodeDb
                if (reportToNutchServer)
                    this.fetchNode = new FetchNode();
                else
                    this.fetchNode = null;

                // check whether must be stopped
                if (isHalted()) {
                    LOG.debug("{} set to halted", getName());
                    fit = null;
                    return;
                }

                fit = fetchQueues.getFetchItem();
                if (fit == null) {
                    if (feeder.isAlive() || fetchQueues.getTotalSize() > 0) {
                        LOG.debug("{} spin-waiting ...", getName());
                        // spin-wait.
                        spinWaiting.incrementAndGet();
                        try {
                            Thread.sleep(500);
                        } catch (Exception ignored) {
                        }
                        spinWaiting.decrementAndGet();
                        continue;
                    } else {
                        // all done, finish this thread
                        LOG.info("Thread {} has no more work available", getName());
                        return;
                    }
                }
                lastRequestStart.set(System.currentTimeMillis());
                Text reprUrlWritable = (Text) fit.datum.getMetaData().get(
                        Nutch.WRITABLE_REPR_URL_KEY);
                if (reprUrlWritable == null) {
                    setReprUrl(fit.url.toString());
                } else {
                    setReprUrl(reprUrlWritable.toString());
                }
                try {
                    // fetch the page
                    redirecting = false;
                    redirectCount = 0;
                    do {
                        if (LOG.isInfoEnabled()) {
                            LOG.info("fetching {} (queue crawl delay={} ms)",
                                    fit.url, fetchQueues.getFetchItemQueue(fit.queueID).crawlDelay);
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("redirectCount={}", redirectCount);
                        }
                        redirecting = false;
                        Protocol protocol = this.protocolFactory.getProtocol(fit.url
                                .toString());
                        BaseRobotRules rules = protocol.getRobotRules(fit.url, fit.datum);
                        if (!rules.isAllowed(fit.u.toString())) {
                            // unblock
                            fetchQueues.finishFetchItem(fit, true);
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Denied by robots.txt: {}", fit.url);
                            }
                            output(fit.url, fit.datum, null,
                                    ProtocolStatus.STATUS_ROBOTS_DENIED,
                                    CrawlDatum.STATUS_FETCH_GONE);
                            robotsDenied.increment(1);
                            continue;
                        }
                        if (rules.getCrawlDelay() > 0) {
                            if (rules.getCrawlDelay() > maxCrawlDelay && maxCrawlDelay >= 0) {
                                // unblock
                                fetchQueues.finishFetchItem(fit, true);
                                LOG.debug("Crawl-Delay for {} too long ({}), skipping",
                                        rules.getCrawlDelay(), fit.url);
                                output(fit.url, fit.datum, null,
                                        ProtocolStatus.STATUS_ROBOTS_DENIED,
                                        CrawlDatum.STATUS_FETCH_GONE);
                                robotsDeniedMaxCrawlDelay.increment(1);
                                continue;
                            } else {
                                FetchItemQueue fiq = fetchQueues
                                        .getFetchItemQueue(fit.queueID);
                                fiq.crawlDelay = rules.getCrawlDelay();
                                if (LOG.isDebugEnabled()) {
                                    LOG.info("Crawl delay for queue: {} is set to {} as per robots.txt. url: {}",
                                            fit.queueID,
                                            fiq.crawlDelay,
                                            fit.url);
                                }
                            }
                        }
                        ProtocolOutput output = protocol.getProtocolOutput(fit.url,
                                fit.datum);
                        ProtocolStatus status = output.getStatus();
                        Content content = output.getContent();
                        ParseStatus parseStatus;
                        // unblock queue
                        fetchQueues.finishFetchItem(fit);

                        String urlString = fit.url.toString();

                        // used for FetchNode
                        if (fetchNode != null) {
                            fetchNode.setStatus(status.getCode());
                            fetchNode.setFetchTime(System.currentTimeMillis());
                            fetchNode.setUrl(fit.url);
                        }

                        context.getCounter("FetcherStatus", status.getName()).increment(1);

                        switch (status.getCode()) {

                            case ProtocolStatus.WOULDBLOCK:
                                // retry ?
                                fetchQueues.addFetchItem(fit);
                                break;

                            case ProtocolStatus.SUCCESS: // got a page
                                parseStatus = output(fit.url, fit.datum, content, status,
                                        CrawlDatum.STATUS_FETCH_SUCCESS, fit.outlinkDepth);
                                updateStatus(content.getContent().length);
                                if (parseStatus != null && parseStatus.isSuccess()
                                        && parseStatus.getMinorCode() == ParseStatus.SUCCESS_REDIRECT) {
                                    String newUrl = parseStatus.getMessage();
                                    int refreshTime = Integer.valueOf(parseStatus.getArgs()[1]);
                                    Text redirUrl = handleRedirect(fit.datum, urlString,
                                            newUrl, refreshTime < Fetcher.PERM_REFRESH_TIME,
                                            Fetcher.CONTENT_REDIR);
                                    if (redirUrl != null) {
                                        queueRedirect(redirUrl, fit);
                                    }
                                }
                                break;

                            case ProtocolStatus.MOVED: // redirect
                            case ProtocolStatus.TEMP_MOVED:
                                int code;
                                boolean temp;
                                if (status.getCode() == ProtocolStatus.MOVED) {
                                    code = CrawlDatum.STATUS_FETCH_REDIR_PERM;
                                    temp = false;
                                } else {
                                    code = CrawlDatum.STATUS_FETCH_REDIR_TEMP;
                                    temp = true;
                                }
                                output(fit.url, fit.datum, content, status, code);
                                String newUrl = status.getMessage();
                                Text redirUrl = handleRedirect(fit.datum, urlString,
                                        newUrl, temp, Fetcher.PROTOCOL_REDIR);
                                if (redirUrl != null) {
                                    queueRedirect(redirUrl, fit);
                                } else {
                                    // stop redirecting
                                    redirecting = false;
                                }
                                break;

                            case ProtocolStatus.EXCEPTION:
                                logError(fit.url, status.getMessage());
                                int killedURLs = fetchQueues.checkExceptionThreshold(fit
                                        .getQueueID());
                                if (killedURLs != 0)
                                    aboveExceptionThresholdInQueue.increment(killedURLs);
              /* FALLTHROUGH */
                            case ProtocolStatus.RETRY: // retry
                            case ProtocolStatus.BLOCKED:
                                output(fit.url, fit.datum, null, status,
                                        CrawlDatum.STATUS_FETCH_RETRY);
                                break;

                            case ProtocolStatus.GONE: // gone
                            case ProtocolStatus.NOTFOUND:
                            case ProtocolStatus.ACCESS_DENIED:
                            case ProtocolStatus.ROBOTS_DENIED:
                                output(fit.url, fit.datum, null, status,
                                        CrawlDatum.STATUS_FETCH_GONE);
                                break;

                            case ProtocolStatus.NOTMODIFIED:
                                output(fit.url, fit.datum, null, status,
                                        CrawlDatum.STATUS_FETCH_NOTMODIFIED);
                                break;

                            default:
                                if (LOG.isWarnEnabled()) {
                                    LOG.warn("Unknown ProtocolStatus: {}", status.getCode());
                                }
                                output(fit.url, fit.datum, null, status,
                                        CrawlDatum.STATUS_FETCH_RETRY);
                        }

                        if (redirecting && redirectCount > maxRedirect) {
                            fetchQueues.finishFetchItem(fit);
                            if (LOG.isInfoEnabled()) {
                                LOG.info(" - redirect count exceeded {}", fit.url);
                            }
                            output(fit.url, fit.datum, null,
                                    ProtocolStatus.STATUS_REDIR_EXCEEDED,
                                    CrawlDatum.STATUS_FETCH_GONE);
                        }

                    } while (redirecting && (redirectCount <= maxRedirect));

                } catch (Throwable t) { // unexpected exception
                    // unblock
                    fetchQueues.finishFetchItem(fit);
                    logError(fit.url, StringUtils.stringifyException(t));
                    output(fit.url, fit.datum, null, ProtocolStatus.STATUS_FAILED,
                            CrawlDatum.STATUS_FETCH_RETRY);
                }
            }

        } catch (Throwable e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("fetcher caught: {}", e.toString());
            }
        } finally {
            if (fit != null)
                fetchQueues.finishFetchItem(fit);
            activeThreads.decrementAndGet(); // count threads
            LOG.info("-finishing thread {}, activeThreads={}", getName(), activeThreads);
        }
    }

    private Text handleRedirect(CrawlDatum datum, String urlString,
                                String newUrl, boolean temp, String redirType)
            throws MalformedURLException, URLFilterException {
        newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
        newUrl = urlFilters.filter(newUrl);

        if (ignoreExternalLinks) {
            try {
                String origHost = new URL(urlString).getHost().toLowerCase();
                String newHost = new URL(newUrl).getHost().toLowerCase();
                if (!origHost.equals(newHost)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(" - ignoring redirect {} from {} to {} because external links are ignored",
                                redirType, urlString, newUrl);
                    }
                    return null;
                }
            } catch (MalformedURLException ignored) {
            }
        }

        if (newUrl != null && !newUrl.equals(urlString)) {
            reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temp);
            Text url = new Text(newUrl);
            if (maxRedirect > 0) {
                redirecting = true;
                redirectCount++;
                if (LOG.isDebugEnabled()) {
                    LOG.debug(" - {} redirect to {} (fetching now)");
                }
                return url;
            } else {
                CrawlDatum newDatum = new CrawlDatum(CrawlDatum.STATUS_LINKED,
                        datum.getFetchInterval(), datum.getScore());
                // transfer existing metadata
                newDatum.getMetaData().putAll(datum.getMetaData());
                try {
                    scfilters.initialScore(url, newDatum);
                } catch (ScoringFilterException e) {
                    e.printStackTrace();
                }
                if (reprUrl != null) {
                    newDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY,
                            new Text(reprUrl));
                }
                output(url, newDatum, null, null, CrawlDatum.STATUS_LINKED);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(" - {} redirect to {} (fetching later)", redirType, url);
                }
                return null;
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(" - {} redirect skipped: {}",
                        redirType, (newUrl != null ? "to same url" : "filtered"));
            }
            return null;
        }
    }

    private void queueRedirect(Text redirUrl, FetchItem fit)
            throws ScoringFilterException {
        CrawlDatum newDatum = new CrawlDatum(CrawlDatum.STATUS_DB_UNFETCHED,
                fit.datum.getFetchInterval(), fit.datum.getScore());
        // transfer all existing metadata to the redirect
        newDatum.getMetaData().putAll(fit.datum.getMetaData());
        scfilters.initialScore(redirUrl, newDatum);
        if (reprUrl != null) {
            newDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY,
                    new Text(reprUrl));
        }
        fit = FetchItem.create(redirUrl, newDatum, queueMode);
        if (fit != null) {
            FetchItemQueue fiq = fetchQueues.getFetchItemQueue(fit.queueID);
            fiq.addInProgressFetchItem(fit);
        } else {
            // stop redirecting
            redirecting = false;
            notCreatedRedirect.increment(1);
        }
    }

    private void logError(Text url, String message) {
        if (LOG.isInfoEnabled()) {
            LOG.info("fetch of {} failed with: {}", url, message);
        }
        errors.incrementAndGet();
    }

    private ParseStatus output(Text key, CrawlDatum datum, Content content,
                               ProtocolStatus pstatus, int status) {

        return output(key, datum, content, pstatus, status, 0);
    }

    private ParseStatus output(Text key, CrawlDatum datum, Content content,
                               ProtocolStatus pstatus, int status, int outlinkDepth) {

        datum.setStatus(status);
        datum.setFetchTime(System.currentTimeMillis());
        if (pstatus != null)
            datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY, pstatus);

        ParseResult parseResult = null;
        if (content != null) {
            Metadata metadata = content.getMetadata();

            // store the guessed content type in the crawldatum
            if (content.getContentType() != null)
                datum.getMetaData().put(new Text(Metadata.CONTENT_TYPE),
                        new Text(content.getContentType()));

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
      /*
       * Note: Fetcher will only follow meta-redirects coming from the
       * original URL.
       */
            if (parsing && status == CrawlDatum.STATUS_FETCH_SUCCESS) {
                if (!skipTruncated
                        || (skipTruncated && !ParseSegmentMapper.isTruncated(content))) {
                    try {
                        parseResult = this.parseUtil.parse(content);
                    } catch (Exception e) {
                        LOG.warn("Error parsing: {}: {}", key, StringUtils.stringifyException(e));
                    }
                }

                if (parseResult == null) {
                    byte[] signature = SignatureFactory.getSignature(configuration)
                            .calculate(content, new ParseStatus().getEmptyParse(configuration));
                    datum.setSignature(signature);
                }
            }

      /*
       * Store status code in content So we can read this value during parsing
       * (as a separate job) and decide to parse or not.
       */
            content.getMetadata().add(Nutch.FETCH_STATUS_KEY,
                    Integer.toString(status));
        }

        try {
            synchronized(context) {
                context.write(key, new NutchWritable(datum));
            }
            if (content != null && storingContent) {
                synchronized (context) {
                    context.write(key, new NutchWritable(content));
                }
            }
            if (parseResult != null) {
                for (Entry<Text, Parse> entry : parseResult) {
                    Text url = entry.getKey();
                    Parse parse = entry.getValue();
                    ParseStatus parseStatus = parse.getData().getStatus();
                    ParseData parseData = parse.getData();

                    if (!parseStatus.isSuccess()) {
                        LOG.warn("Error parsing: {}: {}", key, parseStatus);
                        parse = parseStatus.getEmptyParse(configuration);
                    }

                    // Calculate page signature. For non-parsing fetchers this will
                    // be done in ParseSegment
                    byte[] signature = SignatureFactory.getSignature(configuration)
                            .calculate(content, parse);
                    // Ensure segment name and score are in parseData metadata
                    parseData.getContentMeta().set(Nutch.SEGMENT_NAME_KEY, segmentName);
                    parseData.getContentMeta().set(Nutch.SIGNATURE_KEY,
                            StringUtil.toHexString(signature));
                    // Pass fetch time to content meta
                    parseData.getContentMeta().set(Nutch.FETCH_TIME_KEY,
                            Long.toString(datum.getFetchTime()));
                    if (url.equals(key))
                        datum.setSignature(signature);
                    try {
                        scfilters.passScoreAfterParsing(url, content, parse);
                    } catch (Exception e) {
                        if (LOG.isWarnEnabled()) {
                            LOG.warn("Couldn't pass score, url {} ({})", key, e);
                        }
                    }

                    String fromHost;

                    // collect outlinks for subsequent db update
                    Outlink[] links = parseData.getOutlinks();
                    int outlinksToStore = Math.min(maxOutlinks, links.length);
                    if (ignoreExternalLinks) {
                        try {
                            fromHost = new URL(url.toString()).getHost().toLowerCase();
                        } catch (MalformedURLException e) {
                            fromHost = null;
                        }
                    } else {
                        fromHost = null;
                    }

                    //used by fetchNode
                    if(fetchNode!=null){
                        fetchNode.setOutlinks(links);
                        fetchNode.setTitle(parseData.getTitle());
                        FetchNodeDb.getInstance().put(fetchNode.getUrl().toString(), fetchNode);
                    }
                    int validCount = 0;

                    // Process all outlinks, normalize, filter and deduplicate
                    List<Outlink> outlinkList = new ArrayList<>(outlinksToStore);
                    HashSet<String> outlinks = new HashSet<>(outlinksToStore);
                    for (int i = 0; i < links.length && validCount < outlinksToStore; i++) {
                        String toUrl = links[i].getToUrl();

                        toUrl = ParseOutputFormat.filterNormalize(url.toString(), toUrl,
                                fromHost, ignoreExternalLinks, urlFilters, normalizers);
                        if (toUrl == null) {
                            continue;
                        }

                        validCount++;
                        links[i].setUrl(toUrl);
                        outlinkList.add(links[i]);
                        outlinks.add(toUrl);
                    }

                    // Only process depth N outlinks
                    if (maxOutlinkDepth > 0 && outlinkDepth < maxOutlinkDepth) {
                        outlinksDetected.increment(outlinks.size());

                        // Counter to limit num outlinks to follow per page
                        int outlinkCounter = 0;

                        // Calculate variable number of outlinks by depth using the
                        // divisor (outlinks = Math.floor(divisor / depth * num.links))
                        int maxOutlinksByDepth = (int) Math.floor(outlinksDepthDivisor
                                / (outlinkDepth + 1) * maxOutlinkDepthNumLinks);

                        String followUrl;

                        // Walk over the outlinks and add as new FetchItem to the queues
                        Iterator<String> iter = outlinks.iterator();
                        while (iter.hasNext() && outlinkCounter < maxOutlinkDepthNumLinks) {
                            followUrl = iter.next();

                            // Check whether we'll follow external outlinks
                            if (outlinksIgnoreExternal) {
                                if (!URLUtil.getHost(url.toString()).equals(
                                        URLUtil.getHost(followUrl))) {
                                    continue;
                                }
                            }

                            outlinksFollowing.increment(1);


                            // Create new FetchItem with depth incremented
                            FetchItem fit = FetchItem.create(new Text(followUrl),
                                    new CrawlDatum(CrawlDatum.STATUS_LINKED, interval),
                                    queueMode, outlinkDepth + 1);
                            fetchQueues.addFetchItem(fit);

                            outlinkCounter++;
                        }
                    }

                    // Overwrite the outlinks in ParseData with the normalized and
                    // filtered set
                    parseData.setOutlinks(outlinkList.toArray(new Outlink[outlinkList
                            .size()]));

                    synchronized (context) {
                        context.write(url, new NutchWritable(new ParseImpl(new ParseText(
                                parse.getText()), parseData, parse.isCanonical())));
                    }
                }
            }
        } catch (IOException e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("fetcher caught: {}", e.toString());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }

        // return parse status if it exits
        if (parseResult != null && !parseResult.isEmpty()) {
            Parse p = parseResult.get(content.getUrl());
            if (p != null) {
                context.getCounter("ParserStatus", ParseStatus.majorCodes[p
                        .getData().getStatus().getMajorCode()]).increment(1);
                return p.getData().getStatus();
            }
        }
        return null;
    }

    private void updateStatus(int bytesInPage) throws IOException {
        pages.incrementAndGet();
        bytes.addAndGet(bytesInPage);
    }

    public synchronized void setHalted(boolean halted) {
        this.halted = halted;
    }

    public synchronized boolean isHalted() {
        return halted;
    }

    public String getReprUrl() {
        return reprUrl;
    }

    private void setReprUrl(String urlString) {
        this.reprUrl = urlString;
    }

}
