/**
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

package org.apache.nutch.crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.io.SelectorEntryWritable;
import org.apache.nutch.mapper.CrawlDbUpdaterMapper;
import org.apache.nutch.mapper.SelectorInverseMapper;
import org.apache.nutch.mapper.SelectorMapper;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.partitioner.SelectorPartitioner;
import org.apache.nutch.partitioner.URLPartitioner;
import org.apache.nutch.reducer.CrawlDbUpdaterReducer;
import org.apache.nutch.reducer.PartitionReducer;
import org.apache.nutch.reducer.SelectorReducer;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.hadoop.util.StringUtils.stringifyException;

// rLogging imports

/**
 * Generates a subset of a crawl db to fetch. This version allows to generate
 * fetchlists for several segments in one go. Unlike in the initial version
 * (OldGenerator), the IP resolution is done ONLY on the entries which have been
 * selected for fetching. The URLs are partitioned by IP, domain or host within
 * a segment. We can chose separately how to count the URLS i.e. by domain or
 * host to limit the entries.
 **/
public class Generator extends NutchTool implements Tool {

    public static final Logger LOG = LoggerFactory.getLogger(Generator.class);

    public static final String GENERATE_UPDATE_CRAWLDB = "generate.update.crawldb";
    public static final String GENERATOR_MIN_SCORE = "generate.min.score";
    public static final String GENERATOR_MIN_INTERVAL = "generate.min.interval";
    public static final String GENERATOR_RESTRICT_STATUS = "generate.restrict.status";
    public static final String GENERATOR_FILTER = "generate.filter";
    public static final String GENERATOR_NORMALISE = "generate.normalise";
    public static final String GENERATOR_MAX_COUNT = "generate.max.count";
    public static final String GENERATOR_COUNT_MODE = "generate.count.mode";
    public static final String GENERATOR_COUNT_VALUE_DOMAIN = "domain";
    public static final String GENERATOR_COUNT_VALUE_HOST = "host";
    public static final String GENERATOR_TOP_N = "generate.topN";
    public static final String GENERATOR_CUR_TIME = "generate.curTime";
    public static final String GENERATOR_DELAY = "crawl.gen.delay";
    public static final String GENERATOR_MAX_NUM_SEGMENTS = "generate.max.num.segments";

    public static class DecreasingFloatComparator extends
            FloatWritable.Comparator {

        /** Compares two FloatWritables decreasing. */
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return super.compare(b2, s2, l2, b1, s1, l1);
        }
    }

    /** Sort fetch lists by hash of URL. */
    public static class HashComparator extends WritableComparator {
        public HashComparator() {
            super(Text.class);
        }

        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable a, WritableComparable b) {
            Text url1 = (Text) a;
            Text url2 = (Text) b;
            int hash1 = hash(url1.getBytes(), 0, url1.getLength());
            int hash2 = hash(url2.getBytes(), 0, url2.getLength());
            return (hash1 < hash2 ? -1 : (hash1 == hash2 ? 0 : 1));
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int hash1 = hash(b1, s1, l1);
            int hash2 = hash(b2, s2, l2);
            return (hash1 < hash2 ? -1 : (hash1 == hash2 ? 0 : 1));
        }

        private static int hash(byte[] bytes, int start, int length) {
            int hash = 1;
            // make later bytes more significant in hash code, so that sorting
            // by
            // hashcode correlates less with by-host ordering.
            for (int i = length - 1; i >= 0; i--)
                hash = (31 * hash) + (int) bytes[start + i];
            return hash;
        }
    }

    public Generator() {
    }

    public Generator(Configuration conf) {
        setConf(conf);
    }

    public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
                           long curTime) throws IOException {

        Configuration configuration = getConf();
        boolean filter = configuration.getBoolean(GENERATOR_FILTER, true);
        boolean normalise = configuration.getBoolean(GENERATOR_NORMALISE, true);
        return generate(dbDir, segments, numLists, topN, curTime, filter,
                normalise, false, 1);
    }

    /**
     * old signature used for compatibility - does not specify whether or not to
     * normalise and set the number of segments to 1
     **/
    public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
                           long curTime, boolean filter, boolean force) throws IOException {
        return generate(dbDir, segments, numLists, topN, curTime, filter, true,
                force, 1);
    }

    /**
     * Generate fetchlists in one or more segments. Whether to filter URLs or not
     * is read from the crawl.generate.filter property in the configuration files.
     * If the property is not found, the URLs are filtered. Same for the
     * normalisation.
     *
     * @param dbDir
     *          Crawl database directory
     * @param segments
     *          Segments directory
     * @param numLists
     *          Number of reduce tasks
     * @param topN
     *          Number of top URLs to be selected
     * @param curTime
     *          Current time in milliseconds
     *
     * @return Path to generated segment or null if no entries were selected
     *
     * @throws IOException
     *           When an I/O error occurs
     */
    public Path[] generate(Path dbDir, Path segments, int numLists, long topN,
                           long curTime, boolean filter, boolean norm, boolean force,
                           int maxNumSegments) throws IOException {

        Configuration configuration = getConf();

        Path tempDir = new Path(configuration.get("mapred.temp.dir", ".")
                + "/generate-temp-" + java.util.UUID.randomUUID().toString());

        Path lock = new Path(dbDir, CrawlDb.LOCK_NAME);
        FileSystem fs = FileSystem.get(configuration);
        LockUtil.createLockFile(fs, lock, force);


        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("Generator: starting at {}", sdf.format(start));
        LOG.info("Generator: Selecting best-scoring urls due for fetch.");
        LOG.info("Generator: filtering: {}", filter);
        LOG.info("Generator: normalizing: {}", norm);
        if (topN != Long.MAX_VALUE) {
            LOG.info("Generator: topN: {}", topN);
        }

        // map to inverted subset due for fetch, sort by score
        Job job = Job.getInstance(configuration, "generate: select from " + dbDir);
        job.setJarByClass(Generator.class);

        if (numLists == -1) { // for politeness make
            numLists = configuration.getInt(MRJobConfig.NUM_MAPS, 1); // a partition per fetch task
        }
        if ("local".equals(configuration.get("mapred.job.tracker")) && numLists != 1) {
            // override
            LOG.info("Generator: jobtracker is 'local', generating exactly one partition.");
            numLists = 1;
        }
        configuration.setLong(GENERATOR_CUR_TIME, curTime);
        // record real generation time
        long generateTime = System.currentTimeMillis();
        configuration.setLong(Nutch.GENERATE_TIME_KEY, generateTime);
        configuration.setLong(GENERATOR_TOP_N, topN);
        configuration.setBoolean(GENERATOR_FILTER, filter);
        configuration.setBoolean(GENERATOR_NORMALISE, norm);
        configuration.setInt(GENERATOR_MAX_NUM_SEGMENTS, maxNumSegments);

        FileInputFormat.addInputPath(job, new Path(dbDir, CrawlDb.CURRENT_NAME));
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(SelectorMapper.class);
        job.setPartitionerClass(SelectorPartitioner.class);
        job.setReducerClass(SelectorReducer.class);

        FileOutputFormat.setOutputPath(job, tempDir);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setSortComparatorClass(DecreasingFloatComparator.class);
        job.setOutputValueClass(SelectorEntryWritable.class);

        MultipleOutputs.addNamedOutput(job, "out", SequenceFileOutputFormat.class,
                FloatWritable.class, SelectorEntryWritable.class);
//        job.setOutputFormatClass(GeneratorOutputFormat.class);

        try {
            job.waitForCompletion(true);
        } catch (Exception e) {
            LockUtil.removeLockFile(fs, lock);
            fs.delete(tempDir, true);
            throw new IOException(e);
        }

        // read the subdirectories generated in the temp
        // output and turn them into segments
        List<Path> generatedSegments = new ArrayList<Path>();

        FileStatus[] status = fs.listStatus(tempDir);
        try {
            for (FileStatus stat : status) {
                Path subfetchlist = stat.getPath();
                if (!subfetchlist.getName().startsWith("fetchlist-"))
                    continue;
                // start a new partition job for this segment
                Path newSeg = partitionSegment(segments, subfetchlist, numLists);
                generatedSegments.add(newSeg);
            }
        } catch (Exception e) {
            LOG.warn("Generator: exception while partitioning segments, exiting ...");
            fs.delete(tempDir, true);
            return null;
        }

        if (generatedSegments.size() == 0) {
            LOG.warn("Generator: 0 records selected for fetching, exiting ...");
            LockUtil.removeLockFile(fs, lock);
            fs.delete(tempDir, true);
            return null;
        }

        if (configuration.getBoolean(GENERATE_UPDATE_CRAWLDB, false)) {
            // update the db from tempDir
            Path tempDir2 = new Path(configuration.get("mapred.temp.dir", ".")
                    + "/generate-temp-" + java.util.UUID.randomUUID().toString());

            job = Job.getInstance(configuration, "generate: updatedb " + dbDir);
            job.setJarByClass(Generator.class);
            configuration.setLong(Nutch.GENERATE_TIME_KEY, generateTime);
            for (Path segmpaths : generatedSegments) {
                Path subGenDir = new Path(segmpaths, CrawlDatum.GENERATE_DIR_NAME);
                FileInputFormat.addInputPath(job, subGenDir);
            }
            FileInputFormat.addInputPath(job, new Path(dbDir, CrawlDb.CURRENT_NAME));
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setMapperClass(CrawlDbUpdaterMapper.class);
            job.setReducerClass(CrawlDbUpdaterReducer.class);
            job.setOutputFormatClass(MapFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(CrawlDatum.class);
            FileOutputFormat.setOutputPath(job, tempDir2);
            try {
                job.waitForCompletion(true);
                CrawlDb.install(job, dbDir);
            } catch (Exception e) {
                LockUtil.removeLockFile(fs, lock);
                fs.delete(tempDir, true);
                fs.delete(tempDir2, true);
                throw new IOException(e);
            }
            fs.delete(tempDir2, true);
        }

        LockUtil.removeLockFile(fs, lock);
        fs.delete(tempDir, true);

        long end = System.currentTimeMillis();
        LOG.info("Generator: finished at {}, elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));

        Path[] patharray = new Path[generatedSegments.size()];
        return generatedSegments.toArray(patharray);
    }

    private Path partitionSegment(Path segmentsDir, Path inputDir, int numLists) throws IOException {

        Configuration configuration = getConf();

        // invert again, partition by host/domain/IP, sort by url hash
        if (LOG.isInfoEnabled()) {
            LOG.info("Generator: Partitioning selected urls for politeness.");
        }
        Path segment = new Path(segmentsDir, generateSegmentName());
        Path output = new Path(segment, CrawlDatum.GENERATE_DIR_NAME);

        LOG.info("Generator: segment: {}", segment);

        Job job = Job.getInstance(configuration, "generate: partition " + segment);
        job.setJarByClass(Generator.class);

        configuration.setInt("partition.url.seed", new Random().nextInt());

        FileInputFormat.addInputPath(job, inputDir);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(SelectorInverseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SelectorEntryWritable.class);
        job.setPartitionerClass(URLPartitioner.class);
        job.setReducerClass(PartitionReducer.class);
        job.setNumReduceTasks(numLists);

        FileOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CrawlDatum.class);
        job.setSortComparatorClass(HashComparator.class);
        try {
            job.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return segment;
    }

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public static synchronized String generateSegmentName() {
        try {
            Thread.sleep(1000);
        } catch (Throwable ignored) {
        }
        return sdf.format(new Date(System.currentTimeMillis()));
    }

    /**
     * Generate a fetchlist from the crawldb.
     */
    public static void main(String args[]) throws Exception {
        int res = ToolRunner
                .run(NutchConfiguration.create(), new Generator(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out
                    .println("Usage: Generator <crawldb> <segments_dir> [-force] [-topN N] [-numFetchers numFetchers] [-adddays numDays] [-noFilter] [-noNorm][-maxNumSegments num]");
            return -1;
        }

        Path dbDir = new Path(args[0]);
        Path segmentsDir = new Path(args[1]);
        long curTime = System.currentTimeMillis();
        long topN = Long.MAX_VALUE;
        int numFetchers = -1;
        boolean filter = true;
        boolean norm = true;
        boolean force = false;
        int maxNumSegments = 1;

        for (int i = 2; i < args.length; i++) {
            if ("-topN".equals(args[i])) {
                topN = Long.parseLong(args[i + 1]);
                i++;
            } else if ("-numFetchers".equals(args[i])) {
                numFetchers = Integer.parseInt(args[i + 1]);
                i++;
            } else if ("-adddays".equals(args[i])) {
                long numDays = Integer.parseInt(args[i + 1]);
                curTime += numDays * 1000L * 60 * 60 * 24;
            } else if ("-noFilter".equals(args[i])) {
                filter = false;
            } else if ("-noNorm".equals(args[i])) {
                norm = false;
            } else if ("-force".equals(args[i])) {
                force = true;
            } else if ("-maxNumSegments".equals(args[i])) {
                maxNumSegments = Integer.parseInt(args[i + 1]);
            }

        }

        try {
            Path[] segs = generate(dbDir, segmentsDir, numFetchers, topN, curTime,
                    filter, norm, force, maxNumSegments);
            if (segs == null)
                return 1;
        } catch (Exception e) {
            LOG.error("Generator: {}", stringifyException(e));
            return -1;
        }
        return 0;
    }

    @Override
    public Map<String, Object> run(Map<String, String> args, String crawlId) throws Exception {


        Map<String, Object> results = new HashMap<>();
        String RESULT = "result";
        String crawldb = (args.containsKey("crawldb")) ? args.get("crawldb") : crawlId+"/crawldb";
        Path dbDir = new Path(crawldb);
        String segments_dir = (args.containsKey("segment_dir")) ? args.get("segments_dir") : crawlId+"/segments";
        Path segmentsDir = new Path(segments_dir);
        long curTime = System.currentTimeMillis();
        long topN = Long.MAX_VALUE;
        int numFetchers = -1;
        boolean filter = true;
        boolean norm = true;
        boolean force = false;
        int maxNumSegments = 1;


        if (args.containsKey("topN")) {
            topN = Long.parseLong(args.get("topN"));
        }
        if (args.containsKey("numFetchers")) {
            numFetchers = Integer.parseInt(args.get("numFetchers"));
        }
        if (args.containsKey("adddays")) {
            long numDays = Integer.parseInt(args.get("adddays"));
            curTime += numDays * 1000L * 60 * 60 * 24;
        }
        if (args.containsKey("noFilter")) {
            filter = false;
        }
        if (args.containsKey("noNorm")) {
            norm = false;
        }
        if (args.containsKey("force")) {
            force = true;
        }
        if (args.containsKey("maxNumSegments")) {
            maxNumSegments = Integer.parseInt(args.get("maxNumSegments"));
        }

        try {
            Path[] segs = generate(dbDir, segmentsDir, numFetchers, topN, curTime,
                    filter, norm, force, maxNumSegments);
            if (segs == null){
                results.put(RESULT, Integer.toString(1));
                return results;
            }

        } catch (Exception e) {
            LOG.error("Generator: {}", StringUtils.stringifyException(e));
            results.put(RESULT, Integer.toString(-1));
            return results;
        }
        results.put(RESULT, Integer.toString(0));
        return results;
    }
}
