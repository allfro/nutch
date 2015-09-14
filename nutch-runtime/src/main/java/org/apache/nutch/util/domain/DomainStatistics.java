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

package org.apache.nutch.util.domain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.mapper.DomainStatisticsMapper;
import org.apache.nutch.reducer.DomainStatisticsCombiner;
import org.apache.nutch.reducer.DomainStatisticsReducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;

/**
 * Extracts some very basic statistics about domains from the crawldb
 */
public class DomainStatistics extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory
            .getLogger(DomainStatistics.class);

    public static final int MODE_HOST = 1;
    public static final int MODE_DOMAIN = 2;
    public static final int MODE_SUFFIX = 3;
    public static final int MODE_TLD = 4;

    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.out
                    .println("usage: DomainStatistics inputDirs outDir host|domain|suffix|tld [numOfReducer]");
            return 1;
        }
        String inputDir = args[0];
        String outputDir = args[1];
        int numOfReducers = 1;

        if (args.length > 3) {
            numOfReducers = Integer.parseInt(args[3]);
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("DomainStatistics: starting at {}", sdf.format(start));

        int mode = 0;
        String jobName = "DomainStatistics";
        switch (args[2]) {
            case "host":
                jobName = "Host statistics";
                mode = MODE_HOST;
                break;
            case "domain":
                jobName = "Domain statistics";
                mode = MODE_DOMAIN;
                break;
            case "suffix":
                jobName = "Suffix statistics";
                mode = MODE_SUFFIX;
                break;
            case "tld":
                jobName = "TLD statistics";
                mode = MODE_TLD;
                break;
        }

        Configuration conf = getConf();
        conf.setInt("domain.statistics.mode", mode);
        conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(DomainStatistics.class);

        String[] inputDirsSpecs = inputDir.split(",");
        for (String inputDirsSpec : inputDirsSpecs) {
            FileInputFormat.addInputPath(job, new Path(inputDirsSpec));
        }

        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(DomainStatisticsMapper.class);
        job.setReducerClass(DomainStatisticsReducer.class);
        job.setCombinerClass(DomainStatisticsCombiner.class);
        job.setNumReduceTasks(numOfReducers);

        job.waitForCompletion(true);

        long end = System.currentTimeMillis();
        LOG.info("DomainStatistics: finished at {}, elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(NutchConfiguration.create(), new DomainStatistics(), args);
    }

}
