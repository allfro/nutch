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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.io.LoopSet;
import org.apache.nutch.io.Route;
import org.apache.nutch.mapper.LoopFinalizerMapper;
import org.apache.nutch.mapper.LoopInitializerMapper;
import org.apache.nutch.mapper.LooperMapper;
import org.apache.nutch.reducer.LoopFinalizerReducer;
import org.apache.nutch.reducer.LoopInitializerReducer;
import org.apache.nutch.reducer.LooperReducer;
import org.apache.nutch.util.FSUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Random;

import static org.apache.hadoop.util.StringUtils.stringifyException;

/**
 * The Loops job identifies cycles of loops inside of the web graph. This is
 * then used in the LinkRank program to remove those links from consideration
 * during link analysis.
 *
 * This job will identify both reciprocal links and cycles of 2+ links up to a
 * set depth to check. The Loops job is expensive in both computational and
 * space terms. Because it checks outlinks of outlinks of outlinks for cycles
 * its intermediate output can be extremely large even if the end output is
 * rather small. Because of this the Loops job is optional and if it doesn't
 * exist then it won't be factored into the LinkRank program.
 */
public class Loops extends Configured implements Tool {

    public static final Logger LOG = LoggerFactory.getLogger(Loops.class);
    public static final String LOOPS_DIR = "loops";
    public static final String ROUTES_DIR = "routes";

    /**
     * Runs the various loop jobs.
     */
    public void findLoops(Path webGraphDb) throws IOException, ClassNotFoundException, InterruptedException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        if (LOG.isInfoEnabled()) {
            LOG.info("Loops: starting at {}", sdf.format(start));
            LOG.info("Loops: webgraphdb: {}", webGraphDb);
        }

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        Path outlinkDb = new Path(webGraphDb, WebGraph.OUTLINK_DIR);
        Path nodeDb = new Path(webGraphDb, WebGraph.NODE_DIR);
        Path routes = new Path(webGraphDb, ROUTES_DIR);
        Path tempRoute = new Path(webGraphDb, ROUTES_DIR + "-"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        // run the initializer
        Configuration configuration = new NutchJob(conf);
        Job init = Job.getInstance(configuration, "Initializer: " + webGraphDb);
        init.setJarByClass(Loops.class);
        FileInputFormat.addInputPath(init, outlinkDb);
        FileInputFormat.addInputPath(init, nodeDb);
        init.setInputFormatClass(SequenceFileInputFormat.class);
        init.setMapperClass(LoopInitializerMapper.class);
        init.setReducerClass(LoopInitializerReducer.class);
        init.setMapOutputKeyClass(Text.class);
        init.setMapOutputValueClass(ObjectWritable.class);
        init.setOutputKeyClass(Text.class);
        init.setOutputValueClass(Route.class);
        FileOutputFormat.setOutputPath(init, tempRoute);
        init.setOutputFormatClass(SequenceFileOutputFormat.class);

        try {
            LOG.info("Loops: starting initializer");
            init.waitForCompletion(true);
            LOG.info("Loops: installing initializer {}", routes);
            FSUtils.replace(fs, routes, tempRoute, true);
            LOG.info("Loops: finished initializer");
        } catch (IOException e) {
            LOG.error(StringUtils.stringifyException(e));
            throw e;
        }

        // run the loops job for a maxdepth, default 2, which will find a 3 link
        // loop cycle
        int depth = conf.getInt("link.loops.depth", 2);
        for (int i = 0; i < depth; i++) {

            configuration = new NutchJob(conf);
            Job looper = Job.getInstance(configuration, "Looper: " + (i + 1) + " of " + depth);
            looper.setJarByClass(Loops.class);
            FileInputFormat.addInputPath(looper, outlinkDb);
            FileInputFormat.addInputPath(looper, routes);
            looper.setInputFormatClass(SequenceFileInputFormat.class);
            looper.setMapperClass(LooperMapper.class);
            looper.setReducerClass(LooperReducer.class);
            looper.setMapOutputKeyClass(Text.class);
            looper.setMapOutputValueClass(ObjectWritable.class);
            looper.setOutputKeyClass(Text.class);
            looper.setOutputValueClass(Route.class);
            FileOutputFormat.setOutputPath(looper, tempRoute);
            looper.setOutputFormatClass(SequenceFileOutputFormat.class);
            configuration.setBoolean("last", i == (depth - 1));

            try {
                LOG.info("Loops: starting looper");
                looper.waitForCompletion(true);
                LOG.info("Loops: installing looper {}", routes);
                FSUtils.replace(fs, routes, tempRoute, true);
                LOG.info("Loops: finished looper");
            } catch (IOException e) {
                LOG.error(StringUtils.stringifyException(e));
                throw e;
            }
        }

        // run the finalizer
        configuration = new NutchJob(conf);
        Job finalizer = Job.getInstance(configuration, "Finalizer: " + webGraphDb);
        finalizer.setJarByClass(Loops.class);
        FileInputFormat.addInputPath(finalizer, routes);
        finalizer.setInputFormatClass(SequenceFileInputFormat.class);
        finalizer.setMapperClass(LoopFinalizerMapper.class);
        finalizer.setReducerClass(LoopFinalizerReducer.class);
        finalizer.setMapOutputKeyClass(Text.class);
        finalizer.setMapOutputValueClass(Route.class);
        finalizer.setOutputKeyClass(Text.class);
        finalizer.setOutputValueClass(LoopSet.class);
        FileOutputFormat.setOutputPath(finalizer, new Path(webGraphDb, LOOPS_DIR));
        finalizer.setOutputFormatClass(MapFileOutputFormat.class);

        try {
            LOG.info("Loops: starting finalizer");
            finalizer.waitForCompletion(true);
            LOG.info("Loops: finished finalizer");
        } catch (IOException e) {
            LOG.error(StringUtils.stringifyException(e));
            throw e;
        }
        long end = System.currentTimeMillis();
        LOG.info("Loops: finished at {}, elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new Loops(), args);
        System.exit(res);
    }

    /**
     * Runs the Loops tool.
     */
    public int run(String[] args) throws Exception {

        Options options = new Options();
        OptionBuilder.withArgName("help");
        OptionBuilder.withDescription("show this help message");
        Option helpOpts = OptionBuilder.create("help");
        options.addOption(helpOpts);

        OptionBuilder.withArgName("webgraphdb");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("the web graph database to use");
        Option webGraphDbOpts = OptionBuilder.create("webgraphdb");
        options.addOption(webGraphDbOpts);

        CommandLineParser parser = new GnuParser();
        try {

            CommandLine line = parser.parse(options, args);
            if (line.hasOption("help") || !line.hasOption("webgraphdb")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("Loops", options);
                return -1;
            }

            String webGraphDb = line.getOptionValue("webgraphdb");
            findLoops(new Path(webGraphDb));
            return 0;
        } catch (Exception e) {
            LOG.error("Loops: {}", stringifyException(e));
            return -2;
        }
    }
}
