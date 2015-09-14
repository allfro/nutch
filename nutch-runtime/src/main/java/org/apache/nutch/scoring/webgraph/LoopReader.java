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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.nutch.io.LoopSet;
import org.apache.nutch.util.FSUtils;
import org.apache.nutch.util.NutchConfiguration;

import java.io.IOException;

/**
 * The LoopReader tool prints the loopset information for a single url.
 */
public class LoopReader extends Configured {

  private FileSystem fs;
  private MapFile.Reader[] loopReaders;

  public LoopReader() {
  }

  public LoopReader(Configuration conf) {
    super(conf);
  }

  /**
   * Prints loopset for a single url. The loopset information will show any
   * outlink url the eventually forms a link cycle.
   * 
   * @param webGraphDb
   *          The WebGraph to check for loops
   * @param url
   *          The url to check.
   * 
   * @throws IOException
   *           If an error occurs while printing loopset information.
   */
  public void dumpUrl(Path webGraphDb, String url) throws IOException {

    // open the readers
    fs = FileSystem.get(getConf());
    loopReaders = MapFileOutputFormat.getReaders(fs, new Path(webGraphDb,
        Loops.LOOPS_DIR), getConf());

    // get the loopset for a given url, if any
    Text key = new Text(url);
    LoopSet loop = new LoopSet();
    MapFileOutputFormat.getEntry(loopReaders,
        new HashPartitioner<Text, LoopSet>(), key, loop);

    // print out each loop url in the set
    System.out.println(url + ":");
    for (String loopUrl : loop.getLoopSet()) {
      System.out.println("  " + loopUrl);
    }

    // close the readers
    FSUtils.closeReaders(loopReaders);
  }

  /**
   * Runs the LoopReader tool. For this tool to work the loops job must have
   * already been run on the corresponding WebGraph.
   */
  public static void main(String[] args) throws Exception {

    Options options = new Options();
    OptionBuilder.withArgName("help");
    OptionBuilder.withDescription("show this help message");
    Option helpOpts = OptionBuilder.create("help");
    options.addOption(helpOpts);

    OptionBuilder.withArgName("webgraphdb");
    OptionBuilder.hasArg();
    OptionBuilder.withDescription("the webgraphdb to use");
    Option webGraphOpts = OptionBuilder.create("webgraphdb");
    options.addOption(webGraphOpts);

    OptionBuilder.withArgName("url");
    OptionBuilder.hasOptionalArg();
    OptionBuilder.withDescription("the url to dump");
    Option urlOpts = OptionBuilder.create("url");
    options.addOption(urlOpts);

    CommandLineParser parser = new GnuParser();
    try {

      CommandLine line = parser.parse(options, args);
      if (line.hasOption("help") || !line.hasOption("webgraphdb")
          || !line.hasOption("url")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("WebGraphReader", options);
        return;
      }

      String webGraphDb = line.getOptionValue("webgraphdb");
      String url = line.getOptionValue("url");
      LoopReader reader = new LoopReader(NutchConfiguration.create());
      reader.dumpUrl(new Path(webGraphDb), url);
      return;
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
  }

}