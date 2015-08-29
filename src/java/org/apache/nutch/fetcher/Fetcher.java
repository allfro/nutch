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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.mapper.FetcherMapper;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A queue-based fetcher.
 *
 * <p>
 * This fetcher uses a well-known model of one producer (a QueueFeeder) and many
 * consumers (FetcherThread-s).
 *
 * <p>
 * QueueFeeder reads input fetchlists and populates a set of FetchItemQueue-s,
 * which hold FetchItem-s that describe the items to be fetched. There are as
 * many queues as there are unique hosts, but at any given time the total number
 * of fetch items in all queues is less than a fixed number (currently set to a
 * multiple of the number of threads).
 *
 * <p>
 * As items are consumed from the queues, the QueueFeeder continues to add new
 * input items, so that their total count stays fixed (FetcherThread-s may also
 * add new items to the queues e.g. as a results of redirection) - until all
 * input items are exhausted, at which point the number of items in the queues
 * begins to decrease. When this number reaches 0 fetcher will finish.
 *
 * <p>
 * This fetcher implementation handles per-host blocking itself, instead of
 * delegating this work to protocol-specific plugins. Each per-host queue
 * handles its own "politeness" settings, such as the maximum number of
 * concurrent requests and crawl delay between consecutive requests - and also a
 * list of requests in progress, and the time the last request was finished. As
 * FetcherThread-s ask for new items to be fetched, queues may return eligible
 * items or null if for "politeness" reasons this host's queue is not yet ready.
 *
 * <p>
 * If there are still unfetched items in the queues, but none of the items are
 * ready, FetcherThread-s will spin-wait until either some items become
 * available, or a timeout is reached (at which point the Fetcher will abort,
 * assuming the task is hung).
 *
 * @author Andrzej Bialecki
 */
public class Fetcher extends NutchTool implements Tool {

    public static final int PERM_REFRESH_TIME = 5;

    public static final String CONTENT_REDIR = "content";

    public static final String PROTOCOL_REDIR = "protocol";

    public static final Logger LOG = LoggerFactory.getLogger(Fetcher.class);

    public static class InputFormat extends
            SequenceFileInputFormat<Text, CrawlDatum> {

        /** Don't split inputs, to keep things polite. */
        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {
            List<FileStatus> files = listStatus(job);
            return files
                    .stream()
                    .map(cur -> new FileSplit(cur.getPath(), 0, cur.getLen(), null)).collect(Collectors.toList());
        }
    }

    public Fetcher() {
        super(null);
    }

    public Fetcher(Configuration conf) {
        super(conf);
    }


    public static boolean isParsing(Configuration conf) {
        return conf.getBoolean("fetcher.parse", true);
    }

    public static boolean isStoringContent(Configuration conf) {
        return conf.getBoolean("fetcher.store.content", true);
    }



    public void fetch(Path segment, int threads) throws IOException {

        Configuration configuration = getConf();

        checkConfiguration(configuration);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        if (LOG.isInfoEnabled()) {
            LOG.info("Fetcher: starting at {}", sdf.format(start));
            LOG.info("Fetcher: segment: {}", segment);
        }

        // set the actual time for the timelimit relative
        // to the beginning of the whole job and not of a specific task
        // otherwise it keeps trying again if a task fails
        long timelimit = configuration.getLong("fetcher.timelimit.mins", -1);
        if (timelimit != -1) {
            timelimit = System.currentTimeMillis() + (timelimit * 60 * 1000);
            LOG.info("Fetcher Timelimit set for: {}", timelimit);
            configuration.setLong("fetcher.timelimit", timelimit);
        }

        // Set the time limit after which the throughput threshold feature is
        // enabled
        timelimit = configuration.getLong("fetcher.throughput.threshold.check.after", 10);
        timelimit = System.currentTimeMillis() + (timelimit * 60 * 1000);
        configuration.setLong("fetcher.throughput.threshold.check.after", timelimit);

        int maxOutlinkDepth = configuration.getInt("fetcher.follow.outlinks.depth", -1);
        if (maxOutlinkDepth > 0) {
            LOG.info("Fetcher: following outlinks up to depth: {}", maxOutlinkDepth);

            int maxOutlinkDepthNumLinks = configuration.getInt(
                    "fetcher.follow.outlinks.num.links", 4);
            int outlinksDepthDivisor = configuration.getInt(
                    "fetcher.follow.outlinks.depth.divisor", 2);

            int totalOutlinksToFollow = 0;
            for (int i = 0; i < maxOutlinkDepth; i++) {
                totalOutlinksToFollow += (int) Math.floor(outlinksDepthDivisor
                        / (i + 1) * maxOutlinkDepthNumLinks);
            }

            LOG.info("Fetcher: maximum outlinks to follow: {}", totalOutlinksToFollow);
        }

        Job job = Job.getInstance(configuration, "fetch " + segment);
        job.setJarByClass(Fetcher.class);

        configuration.setInt("fetcher.threads.fetch", threads);
        configuration.set(Nutch.SEGMENT_NAME_KEY, segment.getName());

        // for politeness, don't permit parallel execution of a single task
        job.setSpeculativeExecution(false);

        FileInputFormat.addInputPath(job, new Path(segment,
                CrawlDatum.GENERATE_DIR_NAME));
        job.setInputFormatClass(InputFormat.class);

        job.setMapperClass(FetcherMapper.class);

        FileOutputFormat.setOutputPath(job, segment);
        job.setOutputFormatClass(FetcherOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NutchWritable.class);

        try {
            job.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();
        LOG.info("Fetcher: finished at {}, elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));
    }

    /** Run the fetcher. */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new Fetcher(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        String usage = "Usage: Fetcher <segment> [-threads n]";

        if (args.length < 1) {
            System.err.println(usage);
            return -1;
        }

        Configuration configuration = getConf();

        Path segment = new Path(args[0]);

        int threads = configuration.getInt("fetcher.threads.fetch", 10);

        for (int i = 1; i < args.length; i++) { // parse command line
            if (args[i].equals("-threads")) { // found -threads option
                threads = Integer.parseInt(args[++i]);
            }
        }

        configuration.setInt("fetcher.threads.fetch", threads);

        try {
            fetch(segment, threads);
            return 0;
        } catch (Exception e) {
            LOG.error("Fetcher: " + StringUtils.stringifyException(e));
            return -1;
        }

    }

    private void checkConfiguration(Configuration configuration) {
        // ensure that a value has been set for the agent name
        String agentName = configuration.get("http.agent.name");
        if (agentName == null || agentName.trim().length() == 0) {
            String message = "Fetcher: No agents listed in 'http.agent.name'"
                    + " property.";
            if (LOG.isErrorEnabled()) {
                LOG.error(message);
            }
            throw new IllegalArgumentException(message);
        }
    }


    @Override
    public Map<String, Object> run(Map<String, String> args, String crawlId) throws Exception {

        Map<String, Object> results = new HashMap<String, Object>();
        String RESULT = "result";
        String segment_dir = crawlId+"/segments";
        File segmentsDir = new File(segment_dir);
        File[] segmentsList = segmentsDir.listFiles();
        Configuration configuration = getConf();

        Arrays.sort(segmentsList, (f1, f2) -> {
            if(f1.lastModified()>f2.lastModified())
                return -1;
            else
                return 0;
        });

        Path segment = new Path(segmentsList[0].getPath());

        int threads = configuration.getInt("fetcher.threads.fetch", 10);
        boolean parsing = false;

        // parse command line
        if (args.containsKey("threads")) { // found -threads option
            threads = Integer.parseInt(args.get("threads"));
        }
        configuration.setInt("fetcher.threads.fetch", threads);

        try {
            fetch(segment, threads);
            results.put(RESULT, Integer.toString(0));
            return results;
        } catch (Exception e) {
            LOG.error("Fetcher: " + StringUtils.stringifyException(e));
            results.put(RESULT, Integer.toString(-1));
            return results;
        }
    }

}
