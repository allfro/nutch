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
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.io.*;
import org.apache.nutch.mapper.InverterMapper;
import org.apache.nutch.reducer.LinkDbMergerReducer;
import org.apache.nutch.util.FSUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.apache.hadoop.util.StringUtils.stringifyException;

/**
 * The LinkDumper tool creates a database of node to inlink information that can
 * be read using the nested Reader class. This allows the inlink and scoring
 * state of a single url to be reviewed quickly to determine why a given url is
 * ranking a certain way. This tool is to be used with the LinkRank analysis.
 */
public class LinkDumper extends Configured implements Tool {

    public static final Logger LOG = LoggerFactory.getLogger(LinkDumper.class);
    public static final String DUMP_DIR = "linkdump";

    /**
     * Reader class which will print out the url and all of its inlinks to system
     * out. Each inlinkwill be displayed with its node information including score
     * and number of in and outlinks.
     */
    public static class Reader {

        public static void main(String[] args) throws Exception {

            if (args == null || args.length < 2) {
                System.out.println("LinkDumper$Reader usage: <webgraphdb> <url>");
                return;
            }

            // open the readers for the linkdump directory
            Configuration conf = NutchConfiguration.create();
            FileSystem fs = FileSystem.get(conf);
            Path webGraphDb = new Path(args[0]);
            String url = args[1];
            MapFile.Reader[] readers = MapFileOutputFormat.getReaders(new Path(
                    webGraphDb, DUMP_DIR), conf);

            // get the link nodes for the url
            Text key = new Text(url);
            LinkNodes nodes = new LinkNodes();
            MapFileOutputFormat.getEntry(readers,
                    new HashPartitioner<Text, LinkNodes>(), key, nodes);

            // print out the link nodes
            LinkNode[] linkNodesAr = nodes.getLinks();
            System.out.println(url + ":");
            for (LinkNode node : linkNodesAr) {
                System.out.println("  " + node.getUrl() + " - "
                        + node.getNode().toString());
            }

            // close the readers
            FSUtils.closeReaders(readers);
        }
    }

    /**
     * Inverts outlinks from the WebGraph to inlinks and attaches node
     * information.
     */
    public static class Inverter extends
            Reducer<Text, ObjectWritable, Text, LinkNode> {

        private Configuration conf;

        public void setup(Context context) {
            this.conf = context.getConfiguration();
        }

        @Override
        /**
         * Inverts outlinks to inlinks while attaching node information to the
         * outlink.
         */
        protected void reduce(Text key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {
            String fromUrl = key.toString();
            List<LinkDatum> outlinks = new ArrayList<LinkDatum>();
            Node node = null;
            LoopSet loops = null;

            // loop through all values aggregating outlinks, saving node and loopset
            for (ObjectWritable write: values) {
                Object obj = write.get();
                if (obj instanceof Node) {
                    node = (Node) obj;
                } else if (obj instanceof LinkDatum) {
                    outlinks.add(WritableUtils.clone((LinkDatum) obj, conf));
                } else if (obj instanceof LoopSet) {
                    loops = (LoopSet) obj;
                }
            }

            // only collect if there are outlinks
            int numOutlinks = node.getNumOutlinks();
            if (numOutlinks > 0) {

                Set<String> loopSet = (loops != null) ? loops.getLoopSet() : null;
                for (LinkDatum outlink : outlinks) {
                    String toUrl = outlink.getUrl();

                    // remove any url that is in the loopset, same as LinkRank
                    if (loopSet != null && loopSet.contains(toUrl)) {
                        continue;
                    }

                    // collect the outlink as an inlink with the node
                    context.write(new Text(toUrl), new LinkNode(fromUrl, node));
                }
            }
        }
    }

    /**
     * Runs the inverter and merger jobs of the LinkDumper tool to create the url
     * to inlink node database.
     */
    public void dumpLinks(Path webGraphDb) throws IOException, ClassNotFoundException, InterruptedException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("NodeDumper: starting at {}", sdf.format(start));
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        Path linkdump = new Path(webGraphDb, DUMP_DIR);
        Path nodeDb = new Path(webGraphDb, WebGraph.NODE_DIR);
        Path loopSetDb = new Path(webGraphDb, Loops.LOOPS_DIR);
        boolean loopsExists = fs.exists(loopSetDb);
        Path outlinkDb = new Path(webGraphDb, WebGraph.OUTLINK_DIR);

        // run the inverter job
        Path tempInverted = new Path(webGraphDb, "inverted-"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        Configuration configuration = new NutchJob(conf);
        Job inverter = Job.getInstance(configuration, "LinkDumper: inverter");
        FileInputFormat.addInputPath(inverter, nodeDb);
        if (loopsExists) {
            FileInputFormat.addInputPath(inverter, loopSetDb);
        }
        FileInputFormat.addInputPath(inverter, outlinkDb);
        inverter.setInputFormatClass(SequenceFileInputFormat.class);
        inverter.setMapperClass(InverterMapper.class);
        inverter.setReducerClass(Inverter.class);
        inverter.setMapOutputKeyClass(Text.class);
        inverter.setMapOutputValueClass(ObjectWritable.class);
        inverter.setOutputKeyClass(Text.class);
        inverter.setOutputValueClass(LinkNode.class);
        FileOutputFormat.setOutputPath(inverter, tempInverted);
        inverter.setOutputFormatClass(SequenceFileOutputFormat.class);

        try {
            LOG.info("LinkDumper: running inverter");
            inverter.waitForCompletion(true);
            LOG.info("LinkDumper: finished inverter");
        } catch (IOException e) {
            LOG.error(StringUtils.stringifyException(e));
            throw e;
        }

        // run the merger job
        configuration = new NutchJob(conf);
        Job merger = Job.getInstance(configuration, "LinkDumper: merger");
        FileInputFormat.addInputPath(merger, tempInverted);
        merger.setInputFormatClass(SequenceFileInputFormat.class);
        merger.setReducerClass(LinkDbMergerReducer.class);
        merger.setMapOutputKeyClass(Text.class);
        merger.setMapOutputValueClass(LinkNode.class);
        merger.setOutputKeyClass(Text.class);
        merger.setOutputValueClass(LinkNodes.class);
        FileOutputFormat.setOutputPath(merger, linkdump);
        merger.setOutputFormatClass(MapFileOutputFormat.class);

        try {
            LOG.info("LinkDumper: running merger");
            merger.waitForCompletion(true);
            LOG.info("LinkDumper: finished merger");
        } catch (IOException e) {
            LOG.error(StringUtils.stringifyException(e));
            throw e;
        }

        fs.delete(tempInverted, true);
        long end = System.currentTimeMillis();
        LOG.info("LinkDumper: finished at " + sdf.format(end) + ", elapsed: "
                + TimingUtil.elapsedTime(start, end));
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new LinkDumper(),
                args);
        System.exit(res);
    }

    /**
     * Runs the LinkDumper tool. This simply creates the database, to read the
     * values the nested Reader tool must be used.
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
                formatter.printHelp("LinkDumper", options);
                return -1;
            }

            String webGraphDb = line.getOptionValue("webgraphdb");
            dumpLinks(new Path(webGraphDb));
            return 0;
        } catch (Exception e) {
            LOG.error("LinkDumper: {}", stringifyException(e));
            return -2;
        }
    }
}
