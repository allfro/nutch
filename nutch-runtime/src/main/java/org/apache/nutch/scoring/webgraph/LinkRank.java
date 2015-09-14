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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.io.LinkDatum;
import org.apache.nutch.io.Node;
import org.apache.nutch.mapper.AnalyzerMapper;
import org.apache.nutch.mapper.CounterMapper;
import org.apache.nutch.mapper.InitializerMapper;
import org.apache.nutch.mapper.InverterMapper;
import org.apache.nutch.reducer.AnalyzerReducer;
import org.apache.nutch.reducer.CounterReducer;
import org.apache.nutch.reducer.InverterReducer;
import org.apache.nutch.util.FSUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Random;

import static org.apache.hadoop.util.StringUtils.stringifyException;

public class LinkRank extends Configured implements Tool {

    public static final Logger LOG = LoggerFactory.getLogger(LinkRank.class);
    public static final String NUM_NODES = "_num_nodes_";

    /**
     * Runs the counter job. The counter job determines the number of links in the
     * webgraph. This is used during analysis.
     *
     * @param fs
     *          The job file system.
     * @param webGraphDb
     *          The web graph database to use.
     *
     * @return The number of nodes in the web graph.
     * @throws IOException
     *           If an error occurs while running the counter job.
     */
    private int runCounter(FileSystem fs, Path webGraphDb) throws IOException, ClassNotFoundException, InterruptedException {

        // configure the counter job
        Path numLinksPath = new Path(webGraphDb, NUM_NODES);
        Path nodeDb = new Path(webGraphDb, WebGraph.NODE_DIR);
        Configuration configuration = new NutchJob(getConf());
        Job counter = Job.getInstance(configuration, "LinkRank Counter");
        FileInputFormat.addInputPath(counter, nodeDb);
        FileOutputFormat.setOutputPath(counter, numLinksPath);
        counter.setInputFormatClass(SequenceFileInputFormat.class);
        counter.setMapperClass(CounterMapper.class);
        counter.setCombinerClass(CounterReducer.class);
        counter.setReducerClass(CounterReducer.class);
        counter.setMapOutputKeyClass(Text.class);
        counter.setMapOutputValueClass(LongWritable.class);
        counter.setOutputKeyClass(Text.class);
        counter.setOutputValueClass(LongWritable.class);
        counter.setNumReduceTasks(1);
        counter.setOutputFormatClass(TextOutputFormat.class);
        configuration.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

        // run the counter job, outputs to a single reduce task and file
        LOG.info("Starting link counter job");
        try {
            counter.waitForCompletion(true);
        } catch (IOException e) {
            LOG.error(StringUtils.stringifyException(e));
            throw e;
        }
        LOG.info("Finished link counter job");

        // read the first (and only) line from the file which should be the
        // number of links in the web graph
        LOG.info("Reading numlinks temp file");
        FSDataInputStream readLinks = fs.open(new Path(numLinksPath, "part-00000"));
        BufferedReader buffer = new BufferedReader(new InputStreamReader(readLinks));
        String numLinksLine = buffer.readLine();
        readLinks.close();

        // check if there are links to process, if none, webgraph might be empty
        if (numLinksLine == null || numLinksLine.length() == 0) {
            fs.delete(numLinksPath, true);
            throw new IOException("No links to process, is the webgraph empty?");
        }

        // delete temp file and convert and return the number of links as an int
        LOG.info("Deleting numlinks temp file");
        fs.delete(numLinksPath, true);
        String numLinks = numLinksLine.split("\\s+")[1];
        return Integer.parseInt(numLinks);
    }

    /**
     * Runs the initializer job. The initializer job sets up the nodes with a
     * default starting score for link analysis.
     *
     * @param nodeDb
     *          The node database to use.
     * @param output
     *          The job output directory.
     *
     * @throws IOException
     *           If an error occurs while running the initializer job.
     */
    private void runInitializer(Path nodeDb, Path output) throws IOException, ClassNotFoundException, InterruptedException {

        // configure the initializer
        Configuration configuration = new NutchJob(getConf());
        Job initializer = Job.getInstance(configuration, "LinkAnalysis Initializer");
        FileInputFormat.addInputPath(initializer, nodeDb);
        FileOutputFormat.setOutputPath(initializer, output);
        initializer.setInputFormatClass(SequenceFileInputFormat.class);
        initializer.setMapperClass(InitializerMapper.class);
        initializer.setMapOutputKeyClass(Text.class);
        initializer.setMapOutputValueClass(Node.class);
        initializer.setOutputKeyClass(Text.class);
        initializer.setOutputValueClass(Node.class);
        initializer.setOutputFormatClass(MapFileOutputFormat.class);
        configuration.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
                false);

        // run the initializer
        LOG.info("Starting initialization job");
        try {
            initializer.waitForCompletion(true);
        } catch (IOException e) {
            LOG.error(StringUtils.stringifyException(e));
            throw e;
        }
        LOG.info("Finished initialization job.");
    }

    /**
     * Runs the inverter job. The inverter job flips outlinks to inlinks to be
     * passed into the analysis job.
     *
     * The inverter job takes a link loops database if it exists. It is an
     * optional componenet of link analysis due to its extreme computational and
     * space requirements but it can be very useful is weeding out and eliminating
     * link farms and other spam pages.
     *
     * @param nodeDb
     *          The node database to use.
     * @param outlinkDb
     *          The outlink database to use.
     * @param loopDb
     *          The loop database to use if it exists.
     * @param output
     *          The output directory.
     *
     * @throws IOException
     *           If an error occurs while running the inverter job.
     */
    private void runInverter(Path nodeDb, Path outlinkDb, Path loopDb, Path output)
            throws IOException, ClassNotFoundException, InterruptedException {

        // configure the inverter
        Configuration configuration = new NutchJob(getConf());
        Job inverter = Job.getInstance(configuration, "LinkAnalysis Inverter");
        FileInputFormat.addInputPath(inverter, nodeDb);
        FileInputFormat.addInputPath(inverter, outlinkDb);

        // add the loop database if it exists, isn't null
        if (loopDb != null) {
            FileInputFormat.addInputPath(inverter, loopDb);
        }
        FileOutputFormat.setOutputPath(inverter, output);
        inverter.setInputFormatClass(SequenceFileInputFormat.class);
        inverter.setMapperClass(InverterMapper.class);
        inverter.setReducerClass(InverterReducer.class);
        inverter.setMapOutputKeyClass(Text.class);
        inverter.setMapOutputValueClass(ObjectWritable.class);
        inverter.setOutputKeyClass(Text.class);
        inverter.setOutputValueClass(LinkDatum.class);
        inverter.setOutputFormatClass(SequenceFileOutputFormat.class);
        configuration.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

        // run the inverter job
        LOG.info("Starting inverter job");
        try {
            inverter.waitForCompletion(true);
        } catch (IOException e) {
            LOG.error(StringUtils.stringifyException(e));
            throw e;
        }
        LOG.info("Finished inverter job.");
    }

    /**
     * Runs the link analysis job. The link analysis job applies the link rank
     * formula to create a score per url and stores that score in the NodeDb.
     *
     * Typically the link analysis job is run a number of times to allow the link
     * rank scores to converge.
     *
     * @param nodeDb
     *          The node database from which we are getting previous link rank
     *          scores.
     * @param inverted
     *          The inverted inlinks
     * @param output
     *          The link analysis output.
     * @param iteration
     *          The current iteration number.
     * @param numIterations
     *          The total number of link analysis iterations
     *
     * @throws IOException
     *           If an error occurs during link analysis.
     */
    private void runAnalysis(Path nodeDb, Path inverted, Path output, int iteration, int numIterations, float rankOne)
            throws IOException, ClassNotFoundException, InterruptedException {

        Configuration configuration = new NutchJob(getConf());
        Job analyzer = Job.getInstance(configuration, "LinkAnalysis Analyzer, iteration " + (iteration + 1)
                        + " of " + numIterations);
        configuration.set("link.analyze.iteration", String.valueOf(iteration + 1));
        FileInputFormat.addInputPath(analyzer, nodeDb);
        FileInputFormat.addInputPath(analyzer, inverted);
        FileOutputFormat.setOutputPath(analyzer, output);
        configuration.set("link.analyze.rank.one", String.valueOf(rankOne));
        analyzer.setMapOutputKeyClass(Text.class);
        analyzer.setMapOutputValueClass(ObjectWritable.class);
        analyzer.setInputFormatClass(SequenceFileInputFormat.class);
        analyzer.setMapperClass(AnalyzerMapper.class);
        analyzer.setReducerClass(AnalyzerReducer.class);
        analyzer.setOutputKeyClass(Text.class);
        analyzer.setOutputValueClass(Node.class);
        analyzer.setOutputFormatClass(MapFileOutputFormat.class);
        configuration.setBoolean(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false);

        LOG.info("Starting analysis job");
        try {
            analyzer.waitForCompletion(true);
        } catch (IOException e) {
            LOG.error(StringUtils.stringifyException(e));
            throw e;
        }
        LOG.info("Finished analysis job.");
    }

    /**
     * Default constructor.
     */
    public LinkRank() {
        super();
    }

    /**
     * Configurable constructor.
     */
    public LinkRank(Configuration conf) {
        super(conf);
    }

    public void close() {
    }

    /**
     * Runs the complete link analysis job. The complete job determins rank one
     * score. Then runs through a given number of invert and analyze iterations,
     * by default 10. And finally replaces the NodeDb in the WebGraph with the
     * link rank output.
     *
     * @param webGraphDb
     *          The WebGraph to run link analysis on.
     *
     * @throws IOException
     *           If an error occurs during link analysis.
     */
    public void analyze(Path webGraphDb) throws IOException, InterruptedException, ClassNotFoundException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("Analysis: starting at {}", sdf.format(start));

        // store the link rank under the webgraphdb temporarily, final scores get
        // upddated into the nodedb
        Path linkRank = new Path(webGraphDb, "linkrank");
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        // create the linkrank directory if needed
        if (!fs.exists(linkRank)) {
            fs.mkdirs(linkRank);
        }

        // the webgraph outlink and node database paths
        Path wgOutlinkDb = new Path(webGraphDb, WebGraph.OUTLINK_DIR);
        Path wgNodeDb = new Path(webGraphDb, WebGraph.NODE_DIR);
        Path nodeDb = new Path(linkRank, WebGraph.NODE_DIR);
        Path loopDb = new Path(webGraphDb, Loops.LOOPS_DIR);
        if (!fs.exists(loopDb)) {
            loopDb = null;
        }

        // get the number of total nodes in the webgraph, used for rank one, then
        // initialze all urls with a default score
        int numLinks = runCounter(fs, webGraphDb);
        runInitializer(wgNodeDb, nodeDb);
        float rankOneScore = (1f / (float) numLinks);

        if (LOG.isInfoEnabled()) {
            LOG.info("Analysis: Number of links: {}", numLinks);
            LOG.info("Analysis: Rank One: {}", rankOneScore);
        }

        // run invert and analysis for a given number of iterations to allow the
        // link rank scores to converge
        int numIterations = conf.getInt("link.analyze.num.iterations", 10);
        for (int i = 0; i < numIterations; i++) {

            // the input to inverting is always the previous output from analysis
            LOG.info("Analysis: Starting iteration " + (i + 1) + " of "
                    + numIterations);
            Path tempRank = new Path(linkRank + "-"
                    + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
            fs.mkdirs(tempRank);
            Path tempInverted = new Path(tempRank, "inverted");
            Path tempNodeDb = new Path(tempRank, WebGraph.NODE_DIR);

            // run invert and analysis
            runInverter(nodeDb, wgOutlinkDb, loopDb, tempInverted);
            runAnalysis(nodeDb, tempInverted, tempNodeDb, i, numIterations,
                    rankOneScore);

            // replace the temporary NodeDb with the output from analysis
            LOG.info("Analysis: Installing new link scores");
            FSUtils.replace(fs, linkRank, tempRank, true);
            LOG.info("Analysis: finished iteration " + (i + 1) + " of "
                    + numIterations);
        }

        // replace the NodeDb in the WebGraph with the final output of analysis
        LOG.info("Analysis: Installing web graph nodes");
        FSUtils.replace(fs, wgNodeDb, nodeDb, true);

        // remove the temporary link rank folder
        fs.delete(linkRank, true);
        long end = System.currentTimeMillis();
        LOG.info("Analysis: finished at " + sdf.format(end) + ", elapsed: "
                + TimingUtil.elapsedTime(start, end));
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new LinkRank(), args);
        System.exit(res);
    }

    /**
     * Runs the LinkRank tool.
     */
    public int run(String[] args) throws Exception {

        Options options = new Options();
        OptionBuilder.withArgName("help");
        OptionBuilder.withDescription("show this help message");
        Option helpOpts = OptionBuilder.create("help");
        options.addOption(helpOpts);

        OptionBuilder.withArgName("webgraphdb");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("the web graph db to use");
        Option webgraphOpts = OptionBuilder.create("webgraphdb");
        options.addOption(webgraphOpts);

        CommandLineParser parser = new GnuParser();
        try {

            CommandLine line = parser.parse(options, args);
            if (line.hasOption("help") || !line.hasOption("webgraphdb")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("LinkRank", options);
                return -1;
            }

            String webGraphDb = line.getOptionValue("webgraphdb");

            analyze(new Path(webGraphDb));
            return 0;
        } catch (Exception e) {
            LOG.error("LinkAnalysis: {}", stringifyException(e));
            return -2;
        }
    }
}
