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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.mapper.ScoreUpdaterMapper;
import org.apache.nutch.reducer.ScoreUpdaterReducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Random;

import static org.apache.hadoop.util.StringUtils.stringifyException;

/**
 * Updates the score from the WebGraph node database into the crawl database.
 * Any score that is not in the node database is set to the clear score in the
 * crawl database.
 */
public class ScoreUpdater extends Configured implements Tool {

    public static final Logger LOG = LoggerFactory.getLogger(ScoreUpdater.class);

    /**
     * Updates the inlink score in the web graph node databsae into the crawl
     * database.
     *
     * @param crawlDbDir
     *          The crawl database to update
     * @param webGraphDbDir
     *          The webgraph database to use.
     *
     * @throws IOException
     *           If an error occurs while updating the scores.
     */
    public void update(Path crawlDbDir, Path webGraphDbDir) throws IOException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("ScoreUpdater: starting at {}", sdf.format(start));

        Configuration configuration = getConf();
        FileSystem fs = FileSystem.get(configuration);

        // create a temporary crawldb with the new scores
        LOG.info("Running crawl database update {}", crawlDbDir);
        Path nodeDb = new Path(webGraphDbDir, WebGraph.NODE_DIR);
        Path crawlDbCurrent = new Path(crawlDbDir, CrawlDb.CURRENT_NAME);
        Path newCrawlDb = new Path(crawlDbDir, Integer.toString(new Random()
                .nextInt(Integer.MAX_VALUE)));

        // run the updater job outputting to the temp crawl database
        Job updaterJob = Job.getInstance(configuration);
        updaterJob.setJarByClass(ScoreUpdater.class);
        updaterJob.setJobName("Update CrawlDb from WebGraph");
        FileInputFormat.addInputPath(updaterJob, crawlDbCurrent);
        FileInputFormat.addInputPath(updaterJob, nodeDb);
        FileOutputFormat.setOutputPath(updaterJob, newCrawlDb);
        updaterJob.setInputFormatClass(SequenceFileInputFormat.class);
        updaterJob.setMapperClass(ScoreUpdaterMapper.class);
        updaterJob.setReducerClass(ScoreUpdaterReducer.class);
        updaterJob.setMapOutputKeyClass(Text.class);
        updaterJob.setMapOutputValueClass(ObjectWritable.class);
        updaterJob.setOutputKeyClass(Text.class);
        updaterJob.setOutputValueClass(CrawlDatum.class);
        updaterJob.setOutputFormatClass(MapFileOutputFormat.class);

        try {
            updaterJob.waitForCompletion(true);
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));

            // remove the temp crawldb on error
            if (fs.exists(newCrawlDb)) {
                fs.delete(newCrawlDb, true);
            }
            throw new IOException(e);
        }

        // install the temp crawl database
        LOG.info("ScoreUpdater: installing new crawl database {}", crawlDbDir);
        CrawlDb.install(updaterJob, crawlDbDir);

        long end = System.currentTimeMillis();
        LOG.info("ScoreUpdater: finished at {}, elapsed: {}",
                sdf.format(end),
                TimingUtil.elapsedTime(start, end));
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new ScoreUpdater(), args);
        System.exit(res);
    }

    /**
     * Runs the ScoreUpdater tool.
     */
    public int run(String[] args) throws Exception {

        Options options = new Options();
        OptionBuilder.withArgName("help");
        OptionBuilder.withDescription("show this help message");
        Option helpOpts = OptionBuilder.create("help");
        options.addOption(helpOpts);

        OptionBuilder.withArgName("crawldb");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("the crawldb to use");
        Option crawlDbOpts = OptionBuilder.create("crawldb");
        options.addOption(crawlDbOpts);

        OptionBuilder.withArgName("webgraphdb");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("the webgraphdb to use");
        Option webGraphOpts = OptionBuilder.create("webgraphdb");
        options.addOption(webGraphOpts);

        CommandLineParser parser = new GnuParser();
        try {

            CommandLine line = parser.parse(options, args);
            if (line.hasOption("help") || !line.hasOption("webgraphdb")
                    || !line.hasOption("crawldb")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("ScoreUpdater", options);
                return -1;
            }

            String crawlDb = line.getOptionValue("crawldb");
            String webGraphDb = line.getOptionValue("webgraphdb");
            update(new Path(crawlDb), new Path(webGraphDb));
            return 0;
        } catch (Exception e) {
            LOG.error("ScoreUpdater: {}", stringifyException(e));
            return -1;
        }
    }
}
