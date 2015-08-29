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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.mapper.CrawlDbFilterMapper;
import org.apache.nutch.reducer.CrawlDbReducer;
import org.apache.nutch.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

// Commons Logging imports

/**
 * This class takes the output of the fetcher and updates the crawldb
 * accordingly.
 */
public class CrawlDb extends NutchTool implements Tool {
    public static final Logger LOG = LoggerFactory.getLogger(CrawlDb.class);

    public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";

    public static final String CRAWLDB_PURGE_404 = "db.update.purge.404";

    public static final String CURRENT_NAME = "current";

    public static final String LOCK_NAME = ".locked";

    public CrawlDb() {
    }

    public CrawlDb(Configuration conf) {
        setConf(conf);
    }

    public void update(Path crawlDb, Path[] segments, boolean normalize,
                       boolean filter) throws IOException {
        boolean additionsAllowed = getConf().getBoolean(CRAWLDB_ADDITIONS_ALLOWED,
                true);
        update(crawlDb, segments, normalize, filter, additionsAllowed, false);
    }

    public void update(Path crawlDb, Path[] segments, boolean normalize,
                       boolean filter, boolean additionsAllowed, boolean force)
            throws IOException {
        Configuration configuration = getConf();
        FileSystem fs = FileSystem.get(configuration);
        Path lock = new Path(crawlDb, LOCK_NAME);
        LockUtil.createLockFile(fs, lock, force);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();

        Job job = CrawlDb.createJob(configuration, crawlDb);
        job.setJarByClass(CrawlDb.class);
        configuration.setBoolean(CRAWLDB_ADDITIONS_ALLOWED, additionsAllowed);
        configuration.setBoolean(CrawlDbFilterMapper.URL_FILTERING, filter);
        configuration.setBoolean(CrawlDbFilterMapper.URL_NORMALIZING, normalize);

        boolean url404Purging = configuration.getBoolean(CRAWLDB_PURGE_404, false);

        if (LOG.isInfoEnabled()) {
            LOG.info("CrawlDb update: starting at {}", sdf.format(start));
            LOG.info("CrawlDb update: db: {}", crawlDb);
            LOG.info("CrawlDb update: segments: {}", Arrays.asList(segments));
            LOG.info("CrawlDb update: additions allowed: {}", additionsAllowed);
            LOG.info("CrawlDb update: URL normalizing: {}", normalize);
            LOG.info("CrawlDb update: URL filtering: {}", filter);
            LOG.info("CrawlDb update: 404 purging: {}", url404Purging);
        }

        for (Path segment : segments) {
            Path fetch = new Path(segment, CrawlDatum.FETCH_DIR_NAME);
            Path parse = new Path(segment, CrawlDatum.PARSE_DIR_NAME);
            if (fs.exists(fetch) && fs.exists(parse)) {
                FileInputFormat.addInputPath(job, fetch);
                FileInputFormat.addInputPath(job, parse);
            } else {
                LOG.info(" - skipping invalid segment {}", segment);
            }
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("CrawlDb update: Merging segment data into db.");
        }
        try {
            job.waitForCompletion(true);
        } catch (Exception e) {
            LockUtil.removeLockFile(fs, lock);
            Path outPath = FileOutputFormat.getOutputPath(job);
            if (fs.exists(outPath))
                fs.delete(outPath, true);
            throw new IOException(e);
        }

        CrawlDb.install(job, crawlDb);
        long end = System.currentTimeMillis();
        LOG.info("CrawlDb update: finished at {}, elapsed: ",
                sdf.format(end),
                TimingUtil.elapsedTime(start, end));
    }

    /*
     * Configure a new CrawlDb in a temp folder at crawlDb/<rand>
     */
    public static Job createJob(Configuration configuration, Path crawlDbDir)
            throws IOException {
        Path newCrawlDb = new Path(crawlDbDir, Integer.toString(new Random()
                .nextInt(Integer.MAX_VALUE)));

        Job job = Job.getInstance(configuration, "crawldb " + crawlDbDir);

        Path current = new Path(crawlDbDir, CURRENT_NAME);
        if (FileSystem.get(configuration).exists(current)) {
            FileInputFormat.addInputPath(job, current);
        }
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(CrawlDbFilterMapper.class);
        job.setReducerClass(CrawlDbReducer.class);

        FileOutputFormat.setOutputPath(job, newCrawlDb);
        job.setOutputFormatClass(MapFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CrawlDatum.class);

        // https://issues.apache.org/jira/browse/NUTCH-1110
        configuration.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

        return job;
    }

    public static void install(Job job, Path crawlDb) throws IOException {
        Configuration configuration = job.getConfiguration();
        boolean preserveBackup = configuration.getBoolean("db.preserve.backup", true);

        Path newCrawlDb = FileOutputFormat.getOutputPath(job);
        FileSystem fs = FileSystem.get(configuration);
        Path old = new Path(crawlDb, "old");
        Path current = new Path(crawlDb, CURRENT_NAME);
        if (fs.exists(current)) {
            if (fs.exists(old))
                fs.delete(old, true);
            fs.rename(current, old);
        }
        fs.mkdirs(crawlDb);
        fs.rename(newCrawlDb, current);
        if (!preserveBackup && fs.exists(old))
            fs.delete(old, true);
        Path lock = new Path(crawlDb, LOCK_NAME);
        LockUtil.removeLockFile(fs, lock);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new CrawlDb(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            System.err
                    .println("Usage: CrawlDb <crawldb> (-dir <segments> | <seg1> <seg2> ...) [-force] [-normalize] [-filter] [-noAdditions]");
            System.err.println("\tcrawldb\tCrawlDb to update");
            System.err
                    .println("\t-dir segments\tparent directory containing all segments to update from");
            System.err
                    .println("\tseg1 seg2 ...\tlist of segment names to update from");
            System.err
                    .println("\t-force\tforce update even if CrawlDb appears to be locked (CAUTION advised)");
            System.err
                    .println("\t-normalize\tuse URLNormalizer on urls in CrawlDb and segment (usually not needed)");
            System.err
                    .println("\t-filter\tuse URLFilters on urls in CrawlDb and segment");
            System.err
                    .println("\t-noAdditions\tonly update already existing URLs, don't add any newly discovered URLs");

            return -1;
        }

        Configuration configuration = getConf();

        boolean normalize = configuration.getBoolean(CrawlDbFilterMapper.URL_NORMALIZING,
                false);
        boolean filter = configuration.getBoolean(CrawlDbFilterMapper.URL_FILTERING, false);
        boolean additionsAllowed = configuration.getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);
        boolean force = false;
        final FileSystem fs = FileSystem.get(configuration);

        HashSet<Path> dirs = new HashSet<>();
        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "-normalize":
                    normalize = true;
                    break;
                case "-filter":
                    filter = true;
                    break;
                case "-force":
                    force = true;
                    break;
                case "-noAdditions":
                    additionsAllowed = false;
                    break;
                case "-dir":
                    FileStatus[] paths = fs.listStatus(new Path(args[++i]),
                            HadoopFSUtil.getPassDirectoriesFilter(fs));
                    dirs.addAll(Arrays.asList(HadoopFSUtil.getPaths(paths)));
                    break;
                default:
                    dirs.add(new Path(args[i]));
                    break;
            }
        }
        try {
            update(new Path(args[0]), dirs.toArray(new Path[dirs.size()]), normalize,
                    filter, additionsAllowed, force);
            return 0;
        } catch (Exception e) {
            LOG.error("CrawlDb update: {}", StringUtils.stringifyException(e));
            return -1;
        }
    }

    /*
     * Used for Nutch REST service
     */
    @Override
    public Map<String, Object> run(Map<String, String> args, String crawlId) throws Exception {

        Map<String, Object> results = new HashMap<String, Object>();
        String RESULT = "result";
        boolean normalize = getConf().getBoolean(CrawlDbFilterMapper.URL_NORMALIZING,
                false);
        boolean filter = getConf().getBoolean(CrawlDbFilterMapper.URL_FILTERING, false);
        boolean additionsAllowed = getConf().getBoolean(CRAWLDB_ADDITIONS_ALLOWED,
                true);
        boolean force = false;
        HashSet<Path> dirs = new HashSet<Path>();

        if (args.containsKey("normalize")) {
            normalize = true;
        }
        if (args.containsKey("filter")) {
            filter = true;
        }
        if (args.containsKey("force")) {
            force = true;
        }
        if (args.containsKey("noAdditions")) {
            additionsAllowed = false;
        }

        String crawldb = crawlId+"/crawldb";
        String segment_dir = crawlId+"/segments";
        File segmentsDir = new File(segment_dir);
        File[] segmentsList = segmentsDir.listFiles();
        Arrays.sort(segmentsList, (f1, f2) -> {
            if(f1.lastModified()>f2.lastModified())
                return -1;
            else
                return 0;
        });

        dirs.add(new Path(segmentsList[0].getPath()));

        try {
            update(new Path(crawldb), dirs.toArray(new Path[dirs.size()]), normalize,
                    filter, additionsAllowed, force);
            results.put(RESULT, Integer.toString(0));
            return results;
        } catch (Exception e) {
            LOG.error("CrawlDb update: {}", StringUtils.stringifyException(e));
            results.put(RESULT, Integer.toString(-1));
            return results;
        }
    }
}
