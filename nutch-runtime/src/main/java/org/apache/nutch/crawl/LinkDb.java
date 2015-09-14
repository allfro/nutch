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
import org.apache.nutch.io.Inlinks;
import org.apache.nutch.mapper.LinkDbFilterMapper;
import org.apache.nutch.mapper.LinkDbMapper;
import org.apache.nutch.mapper.LinkDbMergerReducer;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

// Commons Logging imports

/** Maintains an inverted link map, listing incoming links for each url. */
public class LinkDb extends NutchTool implements Tool {

    public static final Logger LOG = LoggerFactory.getLogger(LinkDb.class);

    public static final String IGNORE_INTERNAL_LINKS = "db.ignore.internal.links";
    public static final String IGNORE_EXTERNAL_LINKS = "db.ignore.external.links";

    public static final String CURRENT_NAME = "current";
    public static final String LOCK_NAME = ".locked";

    public LinkDb() {
    }

    public LinkDb(Configuration conf) {
        setConf(conf);
    }

    public void invert(Path linkDb, final Path segmentsDir, boolean normalize,
                       boolean filter, boolean force) throws IOException, InterruptedException, ClassNotFoundException {
        final FileSystem fs = FileSystem.get(getConf());
        FileStatus[] files = fs.listStatus(segmentsDir,
                HadoopFSUtil.getPassDirectoriesFilter(fs));
        invert(linkDb, HadoopFSUtil.getPaths(files), normalize, filter, force);
    }

    public void invert(Path linkDb, Path[] segments, boolean normalize,
                       boolean filter, boolean force) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = getConf();
        Job job = LinkDb.createJob(configuration, linkDb, normalize, filter);
        Path lock = new Path(linkDb, LOCK_NAME);
        FileSystem fs = FileSystem.get(getConf());
        LockUtil.createLockFile(fs, lock, force);
        Path currentLinkDb = new Path(linkDb, CURRENT_NAME);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        if (LOG.isInfoEnabled()) {
            LOG.info("LinkDb: starting at {}", sdf.format(start));
            LOG.info("LinkDb: linkdb: {}", linkDb);
            LOG.info("LinkDb: URL normalize: {}", normalize);
            LOG.info("LinkDb: URL filter: {}", filter);
            if (configuration.getBoolean(IGNORE_INTERNAL_LINKS, true)) {
                LOG.info("LinkDb: internal links will be ignored.");
            }
            if (configuration.getBoolean(IGNORE_EXTERNAL_LINKS, false)) {
                LOG.info("LinkDb: external links will be ignored.");
            }
        }
        if (configuration.getBoolean(IGNORE_INTERNAL_LINKS, true)
                && configuration.getBoolean(IGNORE_EXTERNAL_LINKS, false)) {
            LOG.warn("LinkDb: internal and external links are ignored! "
                    + "Nothing to do, actually. Exiting.");
            LockUtil.removeLockFile(fs, lock);
            return;
        }

        for (Path segment : segments) {
            if (LOG.isInfoEnabled()) {
                LOG.info("LinkDb: adding segment: {}", segment);
            }
            FileInputFormat.addInputPath(job, new Path(segment,
                    ParseData.DIR_NAME));
        }
        try {
            job.waitForCompletion(true);
        } catch (IOException e) {
            LockUtil.removeLockFile(fs, lock);
            throw e;
        }
        if (fs.exists(currentLinkDb)) {
            if (LOG.isInfoEnabled()) {
                LOG.info("LinkDb: merging with existing linkdb: {}", linkDb);
            }
            // try to merge
            Path newLinkDb = FileOutputFormat.getOutputPath(job);
            job = LinkDbMerger.createMergeJob(getConf(), linkDb, normalize, filter);
            FileInputFormat.addInputPath(job, currentLinkDb);
            FileInputFormat.addInputPath(job, newLinkDb);
            try {
                job.waitForCompletion(true);
            } catch (IOException e) {
                LockUtil.removeLockFile(fs, lock);
                fs.delete(newLinkDb, true);
                throw e;
            }
            fs.delete(newLinkDb, true);
        }
        LinkDb.install(job, linkDb);

        long end = System.currentTimeMillis();
        LOG.info("LinkDb: finished at {}, elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));
    }

    private static Job createJob(Configuration config, Path linkDb,
                                 boolean normalize, boolean filter) throws IOException {
        Path newLinkDb = new Path("linkdb-"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        Configuration configuration = new NutchJob(config);
        Job job = Job.getInstance(configuration, "linkdb " + linkDb);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(LinkDbMapper.class);
        job.setCombinerClass(LinkDbMergerReducer.class);
        // if we don't run the mergeJob, perform normalization/filtering now
        if (normalize || filter) {
            try {
                FileSystem fs = FileSystem.get(config);
                if (!fs.exists(linkDb)) {
                    configuration.setBoolean(LinkDbFilterMapper.URL_FILTERING, filter);
                    configuration.setBoolean(LinkDbFilterMapper.URL_NORMALIZING, normalize);
                }
            } catch (Exception e) {
                LOG.warn("LinkDb createJob: {}", e);
            }
        }
        job.setReducerClass(LinkDbMergerReducer.class);

        FileOutputFormat.setOutputPath(job, newLinkDb);
        job.setOutputFormatClass(MapFileOutputFormat.class);
        configuration.setBoolean("mapred.output.compress", true);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Inlinks.class);

        return job;
    }

    public static void install(Job job, Path linkDb) throws IOException {
        Path newLinkDb = FileOutputFormat.getOutputPath(job);
        FileSystem fs = FileSystem.get(job.getConfiguration());
        Path old = new Path(linkDb, "old");
        Path current = new Path(linkDb, CURRENT_NAME);
        if (fs.exists(current)) {
            if (fs.exists(old))
                fs.delete(old, true);
            fs.rename(current, old);
        }
        fs.mkdirs(linkDb);
        fs.rename(newLinkDb, current);
        if (fs.exists(old))
            fs.delete(old, true);
        LockUtil.removeLockFile(fs, new Path(linkDb, LOCK_NAME));
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new LinkDb(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err
                    .println("Usage: LinkDb <linkdb> (-dir <segmentsDir> | <seg1> <seg2> ...) [-force] [-noNormalize] [-noFilter]");
            System.err.println("\tlinkdb\toutput LinkDb to create or update");
            System.err
                    .println("\t-dir segmentsDir\tparent directory of several segments, OR");
            System.err.println("\tseg1 seg2 ...\t list of segment directories");
            System.err
                    .println("\t-force\tforce update even if LinkDb appears to be locked (CAUTION advised)");
            System.err.println("\t-noNormalize\tdon't normalize link URLs");
            System.err.println("\t-noFilter\tdon't apply URLFilters to link URLs");
            return -1;
        }
        final FileSystem fs = FileSystem.get(getConf());
        Path db = new Path(args[0]);
        ArrayList<Path> segs = new ArrayList<Path>();
        boolean filter = true;
        boolean normalize = true;
        boolean force = false;
        for (int i = 1; i < args.length; i++) {
            if (args[i].equals("-dir")) {
                FileStatus[] paths = fs.listStatus(new Path(args[++i]),
                        HadoopFSUtil.getPassDirectoriesFilter(fs));
                segs.addAll(Arrays.asList(HadoopFSUtil.getPaths(paths)));
            } else if (args[i].equalsIgnoreCase("-noNormalize")) {
                normalize = false;
            } else if (args[i].equalsIgnoreCase("-noFilter")) {
                filter = false;
            } else if (args[i].equalsIgnoreCase("-force")) {
                force = true;
            } else
                segs.add(new Path(args[i]));
        }
        try {
            invert(db, segs.toArray(new Path[segs.size()]), normalize, filter, force);
            return 0;
        } catch (Exception e) {
            LOG.error("LinkDb: {}", StringUtils.stringifyException(e));
            return -1;
        }
    }

    /*
     * Used for Nutch REST service
     */
    @Override
    public Map<String, Object> run(Map<String, String> args, String crawlId) throws Exception {
//    if (args.size() < 2) {
//      throw new IllegalArgumentException("Required arguments <linkdb> (-dir <segmentsDir> | <seg1> <seg2> ...) [-force] [-noNormalize] [-noFilter]");
//    }

        Map<String, Object> results = new HashMap<String, Object>();
        String RESULT = "result";
        String linkdb = crawlId + "/linkdb";
        Path db = new Path(linkdb);
        ArrayList<Path> segs = new ArrayList<Path>();
        boolean filter = true;
        boolean normalize = true;
        boolean force = false;
        if (args.containsKey("noNormalize")) {
            normalize = false;
        }
        if (args.containsKey("noFilter")) {
            filter = false;
        }
        if (args.containsKey("force")) {
            force = true;
        }
        String segment_dir = crawlId + "/segments";
        File segmentsDir = new File(segment_dir);
        File[] segmentsList = segmentsDir.listFiles();
        Arrays.sort(segmentsList, (f1, f2) -> {
            if(f1.lastModified()>f2.lastModified())
                return -1;
            else
                return 0;
        });
        segs.add(new Path(segmentsList[0].getPath()));
        try {
            invert(db, segs.toArray(new Path[segs.size()]), normalize, filter, force);
            results.put(RESULT, Integer.toString(0));
            return results;
        } catch (Exception e) {
            LOG.error("LinkDb: {}", StringUtils.stringifyException(e));
            results.put(RESULT, Integer.toString(-1));
            return results;
        }
    }
}