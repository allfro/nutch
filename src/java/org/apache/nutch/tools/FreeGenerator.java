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

package org.apache.nutch.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.io.SelectorEntryWritable;
import org.apache.nutch.mapper.FreeGeneratorMapper;
import org.apache.nutch.partitioner.URLPartitioner;
import org.apache.nutch.reducer.FreeGeneratorReducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

/**
 * This tool generates fetchlists (segments to be fetched) from plain text files
 * containing one URL per line. It's useful when arbitrary URL-s need to be
 * fetched without adding them first to the CrawlDb, or during testing.
 */
public class FreeGenerator extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory
            .getLogger(FreeGenerator.class);

    public static final String FILTER_KEY = "free.generator.filter";
    public static final String NORMALIZE_KEY = "free.generator.normalize";

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err
                    .println("Usage: FreeGenerator <inputDir> <segmentsDir> [-filter] [-normalize]");
            System.err
                    .println("\tinputDir\tinput directory containing one or more input files.");
            System.err
                    .println("\t\tEach text file contains a list of URLs, one URL per line");
            System.err
                    .println("\tsegmentsDir\toutput directory, where new segment will be created");
            System.err.println("\t-filter\trun current URLFilters on input URLs");
            System.err
                    .println("\t-normalize\trun current URLNormalizers on input URLs");
            return -1;
        }
        boolean filter = false;
        boolean normalize = false;
        if (args.length > 2) {
            for (int i = 2; i < args.length; i++) {
                switch (args[i]) {
                    case "-filter":
                        filter = true;
                        break;
                    case "-normalize":
                        normalize = true;
                        break;
                    default:
                        LOG.error("Unknown argument: " + args[i] + ", exiting ...");
                        return -1;
                }
            }
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("FreeGenerator: starting at " + sdf.format(start));

        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FreeGenerator.class);
        configuration.setBoolean(FILTER_KEY, filter);
        configuration.setBoolean(NORMALIZE_KEY, normalize);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(FreeGeneratorMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SelectorEntryWritable.class);
        job.setPartitionerClass(URLPartitioner.class);
        job.setReducerClass(FreeGeneratorReducer.class);
        String segName = Generator.generateSegmentName();
        job.setNumReduceTasks(configuration.getInt(MRJobConfig.NUM_MAPS, 1));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CrawlDatum.class);
        job.setSortComparatorClass(Generator.HashComparator.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1], new Path(segName,
                CrawlDatum.GENERATE_DIR_NAME)));
        try {
            job.waitForCompletion(true);
        } catch (Exception e) {
            LOG.error("FAILED: " + StringUtils.stringifyException(e));
            return -1;
        }
        long end = System.currentTimeMillis();
        LOG.info("FreeGenerator: finished at {}, elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new FreeGenerator(),
                args);
        System.exit(res);
    }
}
