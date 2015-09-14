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
package org.apache.nutch.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.segment.SegmentChecker;
import org.apache.nutch.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.hadoop.util.StringUtils.stringifyException;

/**
 * Generic indexer which relies on the plugins implementing IndexWriter
 **/

public class IndexingJob extends NutchTool implements Tool {

    public static Logger LOG = LoggerFactory.getLogger(IndexingJob.class);

    public IndexingJob() {
        super(null);
    }

    public IndexingJob(Configuration conf) {
        super(conf);
    }

    public void index(Path crawlDb, Path linkDb, List<Path> segments,
                      boolean noCommit) throws IOException, ClassNotFoundException, InterruptedException {
        index(crawlDb, linkDb, segments, noCommit, false, null);
    }

    public void index(Path crawlDb, Path linkDb, List<Path> segments,
                      boolean noCommit, boolean deleteGone)
            throws IOException, ClassNotFoundException, InterruptedException {
        index(crawlDb, linkDb, segments, noCommit, deleteGone, null);
    }

    public void index(Path crawlDb, Path linkDb, List<Path> segments,
                      boolean noCommit, boolean deleteGone, String params)
            throws IOException, InterruptedException, ClassNotFoundException {
        index(crawlDb, linkDb, segments, noCommit, deleteGone, params, false, false);
    }

    public void index(Path crawlDb, Path linkDb, List<Path> segments,
                      boolean noCommit, boolean deleteGone, String params, boolean filter,
                      boolean normalize) throws IOException, ClassNotFoundException, InterruptedException {
        index(crawlDb, linkDb, segments, noCommit, deleteGone, params, false,
                false, false);
    }

    public void index(Path crawlDb, Path linkDb, List<Path> segments,
                      boolean noCommit, boolean deleteGone, String params,
                      boolean filter, boolean normalize, boolean addBinaryContent)
            throws IOException, InterruptedException, ClassNotFoundException {
        index(crawlDb, linkDb, segments, noCommit, deleteGone, params, false,
                false, false, false);
    }

    public void index(Path crawlDb, Path linkDb, List<Path> segments,
                      boolean noCommit, boolean deleteGone, String params,
                      boolean filter, boolean normalize, boolean addBinaryContent,
                      boolean base64) throws IOException, ClassNotFoundException, InterruptedException {


        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("Indexer: starting at {}", sdf.format(start));

        final Configuration configuration = new NutchJob(getConf());
        Job job = Job.getInstance(configuration, "Indexer");

        LOG.info("Indexer: deleting gone documents: {}", deleteGone);
        LOG.info("Indexer: URL filtering: {}", filter);
        LOG.info("Indexer: URL normalizing: {}", normalize);
        if (addBinaryContent) {
            if (base64) {
                LOG.info("Indexer: adding binary content as Base64");
            } else {
                LOG.info("Indexer: adding binary content");
            }
        }
        IndexWriters writers = new IndexWriters(configuration);
        LOG.info(writers.describe());

        Indexer.initMRJob(crawlDb, linkDb, segments, job, addBinaryContent);

        // NOW PASSED ON THE COMMAND LINE AS A HADOOP PARAM
        // job.set(SolrConstants.SERVER_URL, solrUrl);

        configuration.setBoolean(Indexer.INDEXER_DELETE, deleteGone);
        configuration.setBoolean(Indexer.URL_FILTERING, filter);
        configuration.setBoolean(Indexer.URL_NORMALIZING, normalize);
        configuration.setBoolean(Indexer.INDEXER_BINARY_AS_BASE64, base64);

        if (params != null) {
            configuration.set(Indexer.INDEXER_PARAMS, params);
        }

        job.setReduceSpeculativeExecution(false);

        final Path tmp = new Path("tmp_" + System.currentTimeMillis() + "-"
                + new Random().nextInt());

        FileOutputFormat.setOutputPath(job, tmp);
        try {
            job.waitForCompletion(true);
            // do the commits once and for all the reducers in one go
            if (!noCommit) {
                writers.open(configuration, "commit");
                writers.commit();
            }
            LOG.info("Indexer: number of documents indexed, deleted, or skipped:");
            for (Counter counter : job.getCounters().getGroup("IndexerStatus")) {
                LOG.info("Indexer: {} {}",
                        String.format(Locale.ROOT, "%6d", counter.getValue()),
                        counter.getName());
            }
            long end = System.currentTimeMillis();
            LOG.info("Indexer: finished at " + sdf.format(end) + ", elapsed: "
                    + TimingUtil.elapsedTime(start, end));
        } finally {
            FileSystem.get(configuration).delete(tmp, true);
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err
                    //.println("Usage: Indexer <crawldb> [-linkdb <linkdb>] [-params k1=v1&k2=v2...] (<segment> ... | -dir <segments>) [-noCommit] [-deleteGone] [-filter] [-normalize]");
                    .println("Usage: Indexer <crawldb> [-linkdb <linkdb>] [-params k1=v1&k2=v2...] (<segment> ... | -dir <segments>) [-noCommit] [-deleteGone] [-filter] [-normalize] [-addBinaryContent] [-base64]");
            IndexWriters writers = new IndexWriters(getConf());
            System.err.println(writers.describe());
            return -1;
        }

        final Path crawlDb = new Path(args[0]);
        Path linkDb = null;

        final List<Path> segments = new ArrayList<Path>();
        String params = null;

        boolean noCommit = false;
        boolean deleteGone = false;
        boolean filter = false;
        boolean normalize = false;
        boolean addBinaryContent = false;
        boolean base64 = false;

        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "-linkdb":
                    linkDb = new Path(args[++i]);
                    break;
                case "-dir":
                    Path dir = new Path(args[++i]);
                    FileSystem fs = dir.getFileSystem(getConf());
                    FileStatus[] fstats = fs.listStatus(dir,
                            HadoopFSUtil.getPassDirectoriesFilter(fs));
                    Path[] files = HadoopFSUtil.getPaths(fstats);
                    for (Path p : files) {
                        if (SegmentChecker.isIndexable(p, fs)) {
                            segments.add(p);
                        }
                    }
                    break;
                case "-noCommit":
                    noCommit = true;
                    break;
                case "-deleteGone":
                    deleteGone = true;
                    break;
                case "-filter":
                    filter = true;
                    break;
                case "-normalize":
                    normalize = true;
                    break;
                case "-addBinaryContent":
                    addBinaryContent = true;
                    break;
                case "-base64":
                    base64 = true;
                    break;
                case "-params":
                    params = args[++i];
                    break;
                default:
                    segments.add(new Path(args[i]));
                    break;
            }
        }

        try {
            index(crawlDb, linkDb, segments, noCommit, deleteGone, params, filter, normalize, addBinaryContent, base64);
            return 0;
        } catch (final Exception e) {
            LOG.error("Indexer: {}", stringifyException(e));
            return -1;
        }
    }

    public static void main(String[] args) throws Exception {
        final int res = ToolRunner.run(NutchConfiguration.create(),
                new IndexingJob(), args);
        System.exit(res);
    }


    //Used for REST API
    @Override
    public Map<String, Object> run(Map<String, String> args, String crawlId) throws Exception {
        boolean noCommit = false;
        boolean deleteGone = false;
        boolean filter = false;
        boolean normalize = false;
        boolean isSegment = false;
        String params= null;
        Configuration conf = getConf();

        String crawldb = crawlId+"/crawldb";
        Path crawlDb = new Path(crawldb);
        Path linkDb = null;
        List<Path> segments = new ArrayList<Path>();

        if(args.containsKey("linkdb")){
            linkDb = new Path(crawlId+"/linkdb");
        }

        if(args.containsKey("dir")){
            isSegment = true;
            Path dir = new Path(crawlId+"/segments");
            FileSystem fs = dir.getFileSystem(getConf());
            FileStatus[] fstats = fs.listStatus(dir,
                    HadoopFSUtil.getPassDirectoriesFilter(fs));
            Path[] files = HadoopFSUtil.getPaths(fstats);
            for (Path p : files) {
                if (SegmentChecker.isIndexable(p,fs)) {
                    segments.add(p);
                }
            }
        }

        if(args.containsKey("segments")){
            isSegment = true;
            String listOfSegments[] = args.get("segments").split(",");
            for(String s: listOfSegments){
                segments.add(new Path(s));
            }
        }

        if(!isSegment){
            String segment_dir = crawlId+"/segments";
            File segmentsDir = new File(segment_dir);
            File[] segmentsList = segmentsDir.listFiles();
            Arrays.sort(segmentsList, (f1, f2) -> {
                if(f1.lastModified()>f2.lastModified())
                    return -1;
                else
                    return 0;
            });

            Path segment = new Path(segmentsList[0].getPath());
            segments.add(segment);
        }

        if(args.containsKey("noCommit")){
            noCommit = true;
        }
        if(args.containsKey("deleteGone")){
            deleteGone = true;
        }
        if(args.containsKey("normalize")){
            normalize = true;
        }
        if(args.containsKey("filter")){
            filter = true;
        }
        if(args.containsKey("params")){
            params = args.get("params");
        }
        setConf(conf);
        index(crawlDb, linkDb, segments, noCommit, deleteGone, params, filter,
                normalize);
        Map<String, Object> results = new HashMap<>();
        results.put("result", 0);
        return results;
    }
}