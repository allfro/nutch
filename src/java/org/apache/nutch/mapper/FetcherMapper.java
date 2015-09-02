package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.fetcher.FetchItemQueues;
import org.apache.nutch.fetcher.FetcherThread;
import org.apache.nutch.fetcher.QueueFeeder;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.util.SSLUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FetcherMapper extends Mapper<Text, CrawlDatum, Text, NutchWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(FetcherMapper.class);

    private Configuration configuration;
    private Counter hitByThroughputThreshold;
    private Counter bytesDownloaded;

    private AtomicInteger activeThreads = new AtomicInteger(0);
    private AtomicInteger spinWaiting = new AtomicInteger(0);

    private long start = System.currentTimeMillis(); // start time of fetcher run
    private AtomicLong lastRequestStart = new AtomicLong(start);

    private AtomicLong bytes = new AtomicLong(0); // total bytes fetched
    private AtomicInteger pages = new AtomicInteger(0); // total pages fetched
    private AtomicInteger errors = new AtomicInteger(0); // total pages errored

    private boolean storingContent;
    private boolean parsing;
    FetchItemQueues fetchQueues;
    QueueFeeder feeder;

    LinkedList<FetcherThread> fetcherThreads = new LinkedList<>();
    private int threadCount;
    private int queueDepthMuliplier;
    private int throughputThresholdPages;
    private int throughputThresholdMaxRetries;
    private int timeout;
    private long throughputThresholdTimeLimit;
    private int targetBandwidth;
    private int maxNumThreads;
    private int maxThreadsPerQueue;
    private int bandwidthTargetCheckEveryNSecs;
    private long timelimit;
    private Counter hitByTimeLimit;
    private String segmentName;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Crawl all the things! We don't care about SSL certificate validation for a web crawler.
        SSLUtilities.trustAll();

        this.configuration = context.getConfiguration();
        threadCount = configuration.getInt("fetcher.threads.fetch", 10);
        if (LOG.isInfoEnabled()) {
            LOG.info("Fetcher: threads: {}", threadCount);
        }

        int timeoutDivisor = configuration.getInt("fetcher.threads.timeout.divisor", 2);
        if (LOG.isInfoEnabled()) {
            LOG.info("Fetcher: time-out divisor: {}", timeoutDivisor);
        }

        queueDepthMuliplier = configuration.getInt("fetcher.queue.depth.multiplier", 50);

        throughputThresholdPages = configuration.getInt("fetcher.throughput.threshold.pages", -1);
        if (LOG.isInfoEnabled()) {
            LOG.info("Fetcher: throughput threshold: {}", throughputThresholdPages);
        }

        throughputThresholdMaxRetries = configuration.getInt("fetcher.throughput.threshold.retries", 5);
        if (LOG.isInfoEnabled()) {
            LOG.info("Fetcher: throughput threshold retries: {}", throughputThresholdMaxRetries);
        }

        fetchQueues = new FetchItemQueues(configuration);
        hitByThroughputThreshold = context.getCounter("FetcherStatus", "hitByThrougputThreshold");
        bytesDownloaded = context.getCounter("FetcherStatus", "bytes_downloaded");
        hitByTimeLimit = context.getCounter("FetcherStatus", "hitByTimeLimit");

        // select a timeout that avoids a task timeout
        timeout = configuration.getInt("mapred.task.timeout", 10 * 60 * 1000) / timeoutDivisor;

        throughputThresholdTimeLimit = configuration.getLong("fetcher.throughput.threshold.check.after", -1);

        targetBandwidth = configuration.getInt("fetcher.bandwidth.target", -1) * 1000;
        maxNumThreads = configuration.getInt("fetcher.maxNum.threads", threadCount);
        if (maxNumThreads < threadCount) {
            LOG.info("fetcher.maxNum.threads can't be < than {}: using {} instead", threadCount, threadCount);
            maxNumThreads = threadCount;
        }
        bandwidthTargetCheckEveryNSecs = configuration.getInt(
                "fetcher.bandwidth.target.check.everyNSecs", 30);
        if (bandwidthTargetCheckEveryNSecs < 1) {
            LOG.info("fetcher.bandwidth.target.check.everyNSecs can't be < to 1 : using 1 instead");
            bandwidthTargetCheckEveryNSecs = 1;
        }

        // the value of the time limit is either -1 or the time where it should
        // finish
        timelimit = configuration.getLong("fetcher.timelimit", -1);

        maxThreadsPerQueue = configuration.getInt("fetcher.threads.per.queue", 1);

        this.segmentName = configuration.get(Nutch.SEGMENT_NAME_KEY);
        this.storingContent = configuration.getBoolean("fetcher.store.content", true);
        this.parsing = configuration.getBoolean("fetcher.parse", true);;
    }


    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);

        // feeder.setPriority((Thread.MAX_PRIORITY + Thread.NORM_PRIORITY) / 2);
        feeder = new QueueFeeder(context, fetchQueues, threadCount * queueDepthMuliplier);
        System.out.println(timelimit);
        if (timelimit != -1)
            feeder.setTimeLimit(timelimit);
        feeder.start();

        // set non-blocking & no-robots mode for HTTP protocol plugins.
        configuration.setBoolean(Protocol.CHECK_BLOCKING, false);
        configuration.setBoolean(Protocol.CHECK_ROBOTS, false);

        for (int i = 0; i < threadCount; i++) { // spawn threads
            FetcherThread t = new FetcherThread(context, activeThreads, fetchQueues,
                    feeder, spinWaiting, lastRequestStart, errors, segmentName,
                    parsing, storingContent, pages, bytes);
            fetcherThreads.add(t);
            t.start();
        }



        // Used for threshold check, holds pages and bytes processed in the last
        // second
        int pagesLastSec;
        int bytesLastSec;

        // Set to true whenever the threshold has been exceeded for the first time
        int throughputThresholdNumRetries = 0;



        int bandwidthTargetCheckCounter = 0;
        long bytesAtLastBWTCheck = 0l;

        do { // wait for threads to exit
            pagesLastSec = pages.get();
            bytesLastSec = (int) bytes.get();

            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {
            }

            pagesLastSec = pages.get() - pagesLastSec;
            bytesLastSec = (int) bytes.get() - bytesLastSec;

            bytesDownloaded.increment(bytesLastSec);

            reportStatus(context, pagesLastSec, bytesLastSec);

            LOG.info("-activeThreads={}, spinWaiting={}, fetchQueues.totalSize={}, fetchQueues.getQueueCount={}",
                    activeThreads,
                    spinWaiting.get(),
                    fetchQueues.getTotalSize(),
                    fetchQueues.getQueueCount());

            if (!feeder.isAlive() && fetchQueues.getTotalSize() < 5) {
                fetchQueues.dump();
            }

            // if throughput threshold is enabled
            if (throughputThresholdTimeLimit < System.currentTimeMillis()
                    && throughputThresholdPages != -1) {
                // Check if we're dropping below the threshold
                if (pagesLastSec < throughputThresholdPages) {
                    throughputThresholdNumRetries++;
                    LOG.warn("{}: dropping below configured threshold of {} pages per second",
                            throughputThresholdNumRetries,
                            throughputThresholdPages);

                    // Quit if we dropped below threshold too many times
                    if (throughputThresholdNumRetries == throughputThresholdMaxRetries) {
                        LOG.warn("Dropped below threshold too many times, killing!");

                        // Disable the threshold checker
                        throughputThresholdPages = -1;

                        // Empty the queues cleanly and get number of items that were
                        // dropped
                        int itemsDropped = fetchQueues.emptyQueues();

                        if (itemsDropped != 0)
                            this.hitByThroughputThreshold.increment(itemsDropped);
                    }
                }
            }

            // adjust the number of threads if a target bandwidth has been set
            if (targetBandwidth > 0) {
                if (bandwidthTargetCheckCounter < bandwidthTargetCheckEveryNSecs)
                    bandwidthTargetCheckCounter++;
                else if (bandwidthTargetCheckCounter == bandwidthTargetCheckEveryNSecs) {
                    long bpsSinceLastCheck = ((bytes.get() - bytesAtLastBWTCheck) * 8)
                            / bandwidthTargetCheckEveryNSecs;

                    bytesAtLastBWTCheck = bytes.get();
                    bandwidthTargetCheckCounter = 0;

                    int averageBdwPerThread = 0;
                    if (activeThreads.get() > 0)
                        averageBdwPerThread = Math.round(bpsSinceLastCheck / activeThreads.get());

                    LOG.info("averageBdwPerThread : {} kbps", (averageBdwPerThread / 1000));

                    if (bpsSinceLastCheck < targetBandwidth && averageBdwPerThread > 0) {
                        // check whether it is worth doing e.g. more queues than threads

                        if ((fetchQueues.getQueueCount() * maxThreadsPerQueue) > activeThreads.get()) {

                            long remainingBdw = targetBandwidth - bpsSinceLastCheck;
                            int additionalThreads = Math.round(remainingBdw
                                    / averageBdwPerThread);
                            int availableThreads = maxNumThreads - activeThreads.get();

                            // determine the number of available threads (min between
                            // availableThreads and additionalThreads)
                            additionalThreads = (availableThreads < additionalThreads ? availableThreads
                                    : additionalThreads);
                            LOG.info("Has space for more threads ({} vs {} kbps) \t => adding {} new threads",
                                    (bpsSinceLastCheck / 1000),
                                    (targetBandwidth / 1000),
                                    additionalThreads);
                            // activate new threads
                            for (int i = 0; i < additionalThreads; i++) {
                                FetcherThread thread = new FetcherThread(context, activeThreads, fetchQueues,
                                        feeder, spinWaiting, lastRequestStart, errors, segmentName, parsing,
                                        storingContent, pages, bytes);
                                fetcherThreads.add(thread);
                                thread.start();
                            }
                        }
                    } else if (bpsSinceLastCheck > targetBandwidth
                            && averageBdwPerThread > 0) {
                        // if the bandwidth we're using is greater then the expected
                        // bandwidth, we have to stop some threads
                        long excessBdw = bpsSinceLastCheck - targetBandwidth;
                        int excessThreads = Math.round(excessBdw / averageBdwPerThread);
                        LOG.info("Exceeding target bandwidth ({} vs {} kbps). \t=> excessThreads",
                                bpsSinceLastCheck / 1000,
                                (targetBandwidth / 1000),
                                excessThreads);
                        // keep at least one
                        if (excessThreads >= fetcherThreads.size())
                            excessThreads = 0;
                        // de-activates threads
                        for (int i = 0; i < excessThreads; i++) {
                            FetcherThread thread = fetcherThreads.removeLast();
                            thread.setHalted(true);
                        }
                    }
                }
            }

            // check timelimit
            if (!feeder.isAlive()) {
                int itemsDropped = fetchQueues.checkTimelimit();
                if (itemsDropped != 0)
                    hitByTimeLimit.increment(itemsDropped);
            }

            // some requests seem to hang, despite all intentions
            if ((System.currentTimeMillis() - lastRequestStart.get()) > timeout) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("Aborting with {} hung threads.", activeThreads);
                    for (int i = 0; i < fetcherThreads.size(); i++) {
                        FetcherThread thread = fetcherThreads.get(i);
                        if (thread.isAlive()) {
                            LOG.warn("Thread #{} hung while processing {}",
                                    i,
                                    thread.getReprUrl());
                            if (LOG.isDebugEnabled()) {
                                StackTraceElement[] stack = thread.getStackTrace();
                                StringBuilder sb = new StringBuilder();
                                sb.append("Stack of thread #").append(i).append(":\n");
                                for (StackTraceElement s : stack) {
                                    sb.append(s.toString()).append('\n');
                                }
                                LOG.debug(sb.toString());
                            }
                        }
                    }
                }
                return;
            }

        } while (activeThreads.get() > 0);
        LOG.info("-activeThreads={}", activeThreads);

        cleanup(context);
    }

    private void reportStatus(Context context, int pagesLastSec, int bytesLastSec)
            throws IOException {
        Long elapsed = (System.currentTimeMillis() - start) / 1000;

        float avgPagesSec = (float) pages.get() / elapsed.floatValue();
        long avgBytesSec = (bytes.get() / 125l) / elapsed;

        context.setStatus(
                String.format(
                        "%d threads (%d waiting), %d queues, %d URLs queued, %d pages, " +
                                "%d errors, %.2f pages/s (%d last sec), %d kbits/s (%d last sec)",
                        activeThreads.get(),
                        spinWaiting.get(),
                        fetchQueues.getQueueCount(),
                        fetchQueues.getTotalSize(),
                        pages.get(),
                        errors.get(),
                        avgPagesSec,
                        pagesLastSec,
                        avgBytesSec,
                        bytesLastSec / 125

                )
        );
    }

}
