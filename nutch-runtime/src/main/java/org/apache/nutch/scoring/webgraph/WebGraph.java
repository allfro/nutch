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
package org.apache.nutch.scoring.webgraph;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.nutch.io.*;
import org.apache.nutch.mapper.InlinkDbMapper;
import org.apache.nutch.mapper.OutlinkDbMapper;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.reducer.NodeDbReducer;
import org.apache.nutch.reducer.OutlinkDbReducer;
import org.apache.nutch.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.hadoop.util.StringUtils.stringifyException;

/**
 * Creates three databases, one for inlinks, one for outlinks, and a node
 * database that holds the number of in and outlinks to a url and the current
 * score for the url.
 *
 * The score is set by an analysis program such as LinkRank. The WebGraph is an
 * update-able database. Outlinks are stored by their fetch time or by the
 * current system time if no fetch time is available. Only the most recent
 * version of outlinks for a given url is stored. As more crawls are executed
 * and the WebGraph updated, newer Outlinks will replace older Outlinks. This
 * allows the WebGraph to adapt to changes in the link structure of the web.
 *
 * The Inlink database is created from the Outlink database and is regenerated
 * when the WebGraph is updated. The Node database is created from both the
 * Inlink and Outlink databases. Because the Node database is overwritten when
 * the WebGraph is updated and because the Node database holds current scores
 * for urls it is recommended that a crawl-cycle (one or more full crawls) fully
 * complete before the WebGraph is updated and some type of analysis, such as
 * LinkRank, is run to update scores in the Node database in a stable fashion.
 */
public class WebGraph extends Configured implements Tool {

    public static final Logger LOG = LoggerFactory.getLogger(WebGraph.class);
    public static final String LOCK_NAME = ".locked";
    public static final String INLINK_DIR = "inlinks";
    public static final String OUTLINK_DIR = "outlinks/current";
    public static final String OLD_OUTLINK_DIR = "outlinks/old";
    public static final String NODE_DIR = "nodes";

    /**
     * Creates the three different WebGraph databases, Outlinks, Inlinks, and
     * Node. If a current WebGraph exists then it is updated, if it doesn't exist
     * then a new WebGraph database is created.
     *
     * @param webGraphDb
     *          The WebGraph to create or update.
     * @param segments
     *          The array of segments used to update the WebGraph. Newer segments
     *          and fetch times will overwrite older segments.
     * @param normalize
     *          whether to use URLNormalizers on URL's in the segment
     * @param filter
     *          whether to use URLFilters on URL's in the segment
     *
     * @throws IOException
     *           If an error occurs while processing the WebGraph.
     */
    public void createWebGraph(Path webGraphDb, Path[] segments,
                               boolean normalize, boolean filter) throws IOException, ClassNotFoundException, InterruptedException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        if (LOG.isInfoEnabled()) {
            LOG.info("WebGraphDb: starting at {}", sdf.format(start));
            LOG.info("WebGraphDb: webgraphdb: {}", webGraphDb);
            LOG.info("WebGraphDb: URL normalize: {}", normalize);
            LOG.info("WebGraphDb: URL filter: {}", filter);
        }

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // lock an existing webgraphdb to prevent multiple simultaneous updates
        Path lock = new Path(webGraphDb, LOCK_NAME);
        if (!fs.exists(webGraphDb)) {
            fs.mkdirs(webGraphDb);
        }

        LockUtil.createLockFile(fs, lock, false);

        // outlink and temp outlink database paths
        Path outlinkDb = new Path(webGraphDb, OUTLINK_DIR);
        Path oldOutlinkDb = new Path(webGraphDb, OLD_OUTLINK_DIR);

        if (!fs.exists(outlinkDb)) {
            fs.mkdirs(outlinkDb);
        }

        Path tempOutlinkDb = new Path(outlinkDb + "-"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        Configuration configuration = new NutchJob(conf);
        Job outlinkJob = Job.getInstance(configuration, "Outlinkdb: " + outlinkDb);

        boolean deleteGone = conf.getBoolean("link.delete.gone", false);
        boolean preserveBackup = conf.getBoolean("db.preserve.backup", true);

        if (deleteGone) {
            LOG.info("OutlinkDb: deleting gone links");
        }

        // get the parse data and crawl fetch data for all segments
        if (segments != null) {
            for (Path segment : segments) {
                Path parseData = new Path(segment, ParseData.DIR_NAME);
                if (fs.exists(parseData)) {
                    LOG.info("OutlinkDb: adding input: {}", parseData);
                    FileInputFormat.addInputPath(outlinkJob, parseData);
                }

                if (deleteGone) {
                    Path crawlFetch = new Path(segment, CrawlDatum.FETCH_DIR_NAME);
                    if (fs.exists(crawlFetch)) {
                        LOG.info("OutlinkDb: adding input: {}", crawlFetch);
                        FileInputFormat.addInputPath(outlinkJob, crawlFetch);
                    }
                }
            }
        }

        // add the existing webgraph
        LOG.info("OutlinkDb: adding input: {}", outlinkDb);
        FileInputFormat.addInputPath(outlinkJob, outlinkDb);

        configuration.setBoolean(OutlinkDbMapper.URL_NORMALIZING, normalize);
        configuration.setBoolean(OutlinkDbMapper.URL_FILTERING, filter);

        outlinkJob.setInputFormatClass(SequenceFileInputFormat.class);
        outlinkJob.setMapperClass(OutlinkDbMapper.class);
        outlinkJob.setReducerClass(OutlinkDbReducer.class);
        outlinkJob.setMapOutputKeyClass(Text.class);
        outlinkJob.setMapOutputValueClass(NutchWritable.class);
        outlinkJob.setOutputKeyClass(Text.class);
        outlinkJob.setOutputValueClass(LinkDatum.class);
        FileOutputFormat.setOutputPath(outlinkJob, tempOutlinkDb);
        outlinkJob.setOutputFormatClass(MapFileOutputFormat.class);
        configuration.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

        // run the outlinkdb job and replace any old outlinkdb with the new one
        try {
            LOG.info("OutlinkDb: running");
            outlinkJob.waitForCompletion(true);
            LOG.info("OutlinkDb: installing {}", outlinkDb);
            FSUtils.replace(fs, oldOutlinkDb, outlinkDb, true);
            FSUtils.replace(fs, outlinkDb, tempOutlinkDb, true);
            if (!preserveBackup && fs.exists(oldOutlinkDb))
                fs.delete(oldOutlinkDb, true);
            LOG.info("OutlinkDb: finished");
        } catch (IOException e) {

            // remove lock file and and temporary directory if an error occurs
            LockUtil.removeLockFile(fs, lock);
            if (fs.exists(tempOutlinkDb)) {
                fs.delete(tempOutlinkDb, true);
            }
            LOG.error(StringUtils.stringifyException(e));
            throw e;
        }

        // inlink and temp link database paths
        Path inlinkDb = new Path(webGraphDb, INLINK_DIR);
        Path tempInlinkDb = new Path(inlinkDb + "-"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        configuration = new NutchJob(conf);
        Job inlinkJob = Job.getInstance(configuration, "Inlinkdb " + inlinkDb);
        LOG.info("InlinkDb: adding input: {}", outlinkDb);
        FileInputFormat.addInputPath(inlinkJob, outlinkDb);
        inlinkJob.setInputFormatClass(SequenceFileInputFormat.class);
        inlinkJob.setMapperClass(InlinkDbMapper.class);
        inlinkJob.setMapOutputKeyClass(Text.class);
        inlinkJob.setMapOutputValueClass(LinkDatum.class);
        inlinkJob.setOutputKeyClass(Text.class);
        inlinkJob.setOutputValueClass(LinkDatum.class);
        FileOutputFormat.setOutputPath(inlinkJob, tempInlinkDb);
        inlinkJob.setOutputFormatClass(MapFileOutputFormat.class);
        configuration.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
                false);

        try {

            // run the inlink and replace any old with new
            LOG.info("InlinkDb: running");
            inlinkJob.waitForCompletion(true);
            LOG.info("InlinkDb: installing {}", inlinkDb);
            FSUtils.replace(fs, inlinkDb, tempInlinkDb, true);
            LOG.info("InlinkDb: finished");
        } catch (IOException e) {

            // remove lock file and and temporary directory if an error occurs
            LockUtil.removeLockFile(fs, lock);
            if (fs.exists(tempInlinkDb)) {
                fs.delete(tempInlinkDb, true);
            }
            LOG.error(StringUtils.stringifyException(e));
            throw e;
        }

        // node and temp node database paths
        Path nodeDb = new Path(webGraphDb, NODE_DIR);
        Path tempNodeDb = new Path(nodeDb + "-"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        configuration = new NutchJob(conf);
        Job nodeJob = Job.getInstance(configuration, "NodeDb " + nodeDb);
        LOG.info("NodeDb: adding input: {}", outlinkDb);
        LOG.info("NodeDb: adding input: {}", inlinkDb);
        FileInputFormat.addInputPath(nodeJob, outlinkDb);
        FileInputFormat.addInputPath(nodeJob, inlinkDb);
        nodeJob.setInputFormatClass(SequenceFileInputFormat.class);
        nodeJob.setReducerClass(NodeDbReducer.class);
        nodeJob.setMapOutputKeyClass(Text.class);
        nodeJob.setMapOutputValueClass(LinkDatum.class);
        nodeJob.setOutputKeyClass(Text.class);
        nodeJob.setOutputValueClass(Node.class);
        FileOutputFormat.setOutputPath(nodeJob, tempNodeDb);
        nodeJob.setOutputFormatClass(MapFileOutputFormat.class);
        configuration.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
                false);

        try {

            // run the node job and replace old nodedb with new
            LOG.info("NodeDb: running");
            nodeJob.waitForCompletion(true);
            LOG.info("NodeDb: installing {}", nodeDb);
            FSUtils.replace(fs, nodeDb, tempNodeDb, true);
            LOG.info("NodeDb: finished");
        } catch (IOException e) {

            // remove lock file and and temporary directory if an error occurs
            LockUtil.removeLockFile(fs, lock);
            if (fs.exists(tempNodeDb)) {
                fs.delete(tempNodeDb, true);
            }
            LOG.error(StringUtils.stringifyException(e));
            throw e;
        }

        // remove the lock file for the webgraph
        LockUtil.removeLockFile(fs, lock);

        long end = System.currentTimeMillis();
        LOG.info("WebGraphDb: finished at " + sdf.format(end) + ", elapsed: "
                + TimingUtil.elapsedTime(start, end));
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new WebGraph(), args);
        System.exit(res);
    }

    /**
     * Parses command link arguments and runs the WebGraph jobs.
     */
    public int run(String[] args) throws Exception {

        // boolean options
        Option helpOpt = new Option("h", "help", false, "show this help message");
        Option normOpt = new Option("n", "normalize", false,
                "whether to use URLNormalizers on the URL's in the segment");
        Option filtOpt = new Option("f", "filter", false,
                "whether to use URLFilters on the URL's in the segment");

        // argument options
        @SuppressWarnings("static-access")
        Option graphOpt = OptionBuilder
                .withArgName("webgraphdb")
                .hasArg()
                .withDescription(
                        "the web graph database to create (if none exists) or use if one does")
                .create("webgraphdb");
        @SuppressWarnings("static-access")
        Option segOpt = OptionBuilder.withArgName("segment").hasArgs()
                .withDescription("the segment(s) to use").create("segment");
        @SuppressWarnings("static-access")
        Option segDirOpt = OptionBuilder.withArgName("segmentDir").hasArgs()
                .withDescription("the segment directory to use").create("segmentDir");

        // create the options
        Options options = new Options();
        options.addOption(helpOpt);
        options.addOption(normOpt);
        options.addOption(filtOpt);
        options.addOption(graphOpt);
        options.addOption(segOpt);
        options.addOption(segDirOpt);

        CommandLineParser parser = new GnuParser();
        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption("help") || !line.hasOption("webgraphdb")
                    || (!line.hasOption("segment") && !line.hasOption("segmentDir"))) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("WebGraph", options, true);
                return -1;
            }

            String webGraphDb = line.getOptionValue("webgraphdb");

            Path[] segPaths = null;

            // Handle segment option
            if (line.hasOption("segment")) {
                String[] segments = line.getOptionValues("segment");
                segPaths = new Path[segments.length];
                for (int i = 0; i < segments.length; i++) {
                    segPaths[i] = new Path(segments[i]);
                }
            }

            // Handle segmentDir option
            if (line.hasOption("segmentDir")) {
                Path dir = new Path(line.getOptionValue("segmentDir"));
                FileSystem fs = dir.getFileSystem(getConf());
                FileStatus[] fstats = fs.listStatus(dir,
                        HadoopFSUtil.getPassDirectoriesFilter(fs));
                segPaths = HadoopFSUtil.getPaths(fstats);
            }

            boolean normalize = false;

            if (line.hasOption("normalize")) {
                normalize = true;
            }

            boolean filter = false;

            if (line.hasOption("filter")) {
                filter = true;
            }

            createWebGraph(new Path(webGraphDb), segPaths, normalize, filter);
            return 0;
        } catch (Exception e) {
            LOG.error("WebGraph: {}", stringifyException(e));
            return -2;
        }
    }

}
