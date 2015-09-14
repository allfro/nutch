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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.mapper.NodeDumperMapper;
import org.apache.nutch.mapper.NodeDumperSorterMapper;
import org.apache.nutch.reducer.NodeDumperReducer;
import org.apache.nutch.reducer.NodeDumperSorterReducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;

import static org.apache.hadoop.util.StringUtils.stringifyException;

/**
 * A tools that dumps out the top urls by number of inlinks, number of outlinks,
 * or by score, to a text file. One of the major uses of this tool is to check
 * the top scoring urls of a link analysis program such as LinkRank.
 *
 * For number of inlinks or number of outlinks the WebGraph program will need to
 * have been run. For link analysis score a program such as LinkRank will need
 * to have been run which updates the NodeDb of the WebGraph.
 */
public class NodeDumper extends Configured implements Tool {

    public static final Logger LOG = LoggerFactory.getLogger(NodeDumper.class);

    private static enum DumpType {
        INLINKS, OUTLINKS, SCORES
    }

    private static enum AggrType {
        SUM, MAX
    }

    private static enum NameType {
        HOST, DOMAIN
    }

    /**
     * Runs the process to dump the top urls out to a text file.
     *
     * @param webGraphDb
     *          The WebGraph from which to pull values.
     *
     * @param topN
     * @param output
     *
     * @throws IOException
     *           If an error occurs while dumping the top values.
     */
    public void dumpNodes(Path webGraphDb, DumpType type, long topN, Path output,
                          boolean asEff, NameType nameType, AggrType aggrType,
                          boolean asSequenceFile) throws Exception {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("NodeDumper: starting at {}", sdf.format(start));
        Path nodeDb = new Path(webGraphDb, WebGraph.NODE_DIR);
        Configuration conf = getConf();

        Configuration configuration = new NutchJob(conf);
        Job dumper = Job.getInstance(configuration, "NodeDumper: " + webGraphDb);
        dumper.setJarByClass(NodeDumper.class);

        FileInputFormat.addInputPath(dumper, nodeDb);
        dumper.setInputFormatClass(SequenceFileInputFormat.class);

        if (nameType == null) {
            dumper.setMapperClass(NodeDumperSorterMapper.class);
            dumper.setReducerClass(NodeDumperSorterReducer.class);
            dumper.setMapOutputKeyClass(FloatWritable.class);
            dumper.setMapOutputValueClass(Text.class);
        } else {
            dumper.setMapperClass(NodeDumperMapper.class);
            dumper.setReducerClass(NodeDumperReducer.class);
            dumper.setMapOutputKeyClass(Text.class);
            dumper.setMapOutputValueClass(FloatWritable.class);
        }

        dumper.setOutputKeyClass(Text.class);
        dumper.setOutputValueClass(FloatWritable.class);
        FileOutputFormat.setOutputPath(dumper, output);

        if (asSequenceFile) {
            dumper.setOutputFormatClass(SequenceFileOutputFormat.class);
        } else {
            dumper.setOutputFormatClass(TextOutputFormat.class);
        }

        dumper.setNumReduceTasks(1);
        configuration.setBoolean("inlinks", type == DumpType.INLINKS);
        configuration.setBoolean("outlinks", type == DumpType.OUTLINKS);
        configuration.setBoolean("scores", type == DumpType.SCORES);

        configuration.setBoolean("host", nameType == NameType.HOST);
        configuration.setBoolean("domain", nameType == NameType.DOMAIN);
        configuration.setBoolean("sum", aggrType == AggrType.SUM);
        configuration.setBoolean("max", aggrType == AggrType.MAX);

        configuration.setLong("topn", topN);

        // Set equals-sign as separator for Solr's ExternalFileField
        if (asEff) {
            configuration.set("mapred.textoutputformat.separator", "=");
        }

        try {
            LOG.info("NodeDumper: running");
            dumper.waitForCompletion(true);
        } catch (IOException e) {
            LOG.error(StringUtils.stringifyException(e));
            throw e;
        }
        long end = System.currentTimeMillis();
        LOG.info("NodeDumper: finished at {}, elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new NodeDumper(),
                args);
        System.exit(res);
    }

    /**
     * Runs the node dumper tool.
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

        OptionBuilder.withArgName("inlinks");
        OptionBuilder.withDescription("show highest inlinks");
        Option inlinkOpts = OptionBuilder.create("inlinks");
        options.addOption(inlinkOpts);

        OptionBuilder.withArgName("outlinks");
        OptionBuilder.withDescription("show highest outlinks");
        Option outlinkOpts = OptionBuilder.create("outlinks");
        options.addOption(outlinkOpts);

        OptionBuilder.withArgName("scores");
        OptionBuilder.withDescription("show highest scores");
        Option scoreOpts = OptionBuilder.create("scores");
        options.addOption(scoreOpts);

        OptionBuilder.withArgName("topn");
        OptionBuilder.hasOptionalArg();
        OptionBuilder.withDescription("show topN scores");
        Option topNOpts = OptionBuilder.create("topn");
        options.addOption(topNOpts);

        OptionBuilder.withArgName("output");
        OptionBuilder.hasArg();
        OptionBuilder.withDescription("the output directory to use");
        Option outputOpts = OptionBuilder.create("output");
        options.addOption(outputOpts);

        OptionBuilder.withArgName("asEff");
        OptionBuilder
                .withDescription("Solr ExternalFileField compatible output format");
        Option effOpts = OptionBuilder.create("asEff");
        options.addOption(effOpts);

        OptionBuilder.hasArgs(2);
        OptionBuilder.withDescription("group <host|domain> <sum|max>");
        Option groupOpts = OptionBuilder.create("group");
        options.addOption(groupOpts);

        OptionBuilder.withArgName("asSequenceFile");
        OptionBuilder.withDescription("whether to output as a sequencefile");
        Option sequenceFileOpts = OptionBuilder.create("asSequenceFile");
        options.addOption(sequenceFileOpts);

        CommandLineParser parser = new GnuParser();
        try {

            CommandLine line = parser.parse(options, args);
            if (line.hasOption("help") || !line.hasOption("webgraphdb")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("NodeDumper", options);
                return -1;
            }

            String webGraphDb = line.getOptionValue("webgraphdb");
            boolean inlinks = line.hasOption("inlinks");
            boolean outlinks = line.hasOption("outlinks");

            long topN = (line.hasOption("topn") ? Long.parseLong(line
                    .getOptionValue("topn")) : Long.MAX_VALUE);

            // get the correct dump type
            String output = line.getOptionValue("output");
            DumpType type = (inlinks ? DumpType.INLINKS
                    : outlinks ? DumpType.OUTLINKS : DumpType.SCORES);

            NameType nameType = null;
            AggrType aggrType = null;
            String[] group = line.getOptionValues("group");
            if (group != null && group.length == 2) {
                nameType = (group[0].equals("host") ? NameType.HOST : group[0]
                        .equals("domain") ? NameType.DOMAIN : null);
                aggrType = (group[1].equals("sum") ? AggrType.SUM : group[1]
                        .equals("sum") ? AggrType.MAX : null);
            }

            // Use ExternalFileField?
            boolean asEff = line.hasOption("asEff");
            boolean asSequenceFile = line.hasOption("asSequenceFile");

            dumpNodes(new Path(webGraphDb), type, topN, new Path(output), asEff,
                    nameType, aggrType, asSequenceFile);
            return 0;
        } catch (Exception e) {
            LOG.error("NodeDumper: {}", stringifyException(e));
            return -2;
        }
    }
}