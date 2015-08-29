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
package org.apache.nutch.crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.mapper.DBFilterMapper;
import org.apache.nutch.reducer.DedupReducer;
import org.apache.nutch.reducer.StatusUpdateReducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Generic deduplicator which groups fetched URLs with the same digest and marks
 * all of them as duplicate except the one with the highest score (based on the
 * score in the crawldb, which is not necessarily the same as the score
 * indexed). If two (or more) documents have the same score, then the document
 * with the latest timestamp is kept. If the documents have the same timestamp
 * then the one with the shortest URL is kept. The documents marked as duplicate
 * can then be deleted with the command CleaningJob.
 ***/
public class DeduplicationJob extends NutchTool implements Tool {

    public static final Logger LOG = LoggerFactory
            .getLogger(DeduplicationJob.class);

    public final static Text URL_KEY = new Text("_URLTEMPKEY_");

    public int run(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: DeduplicationJob <crawldb>");
            return 1;
        }

        String crawldb = args[0];

        Configuration configuration = getConf();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("DeduplicationJob: starting at " + sdf.format(start));


        Path tempDir = new Path(getConf().get("mapred.temp.dir", ".")
                + "/dedup-temp-"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        Job job = Job.getInstance(configuration, "Deduplication on " + crawldb);
        job.setJarByClass(DeduplicationJob.class);

        FileInputFormat.addInputPath(job, new Path(crawldb, CrawlDb.CURRENT_NAME));
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileOutputFormat.setOutputPath(job, tempDir);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(BytesWritable.class);
        job.setMapOutputValueClass(CrawlDatum.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CrawlDatum.class);

        job.setMapperClass(DBFilterMapper.class);
        job.setReducerClass(DedupReducer.class);

        try {
            job.waitForCompletion(true);
            LOG.info("Deduplication: {} documents marked as duplicates",
                    job.getCounters().findCounter("DeduplicationJobStatus", "Documents marked as duplicate"));
        } catch (final Exception e) {
            LOG.error("DeduplicationJob: {}", StringUtils.stringifyException(e));
            return -1;
        }

        // merge with existing crawl db
        if (LOG.isInfoEnabled()) {
            LOG.info("Deduplication: Updating status of duplicate urls into crawl db.");
        }

        Path dbPath = new Path(crawldb);
        Job mergeJob = CrawlDb.createJob(getConf(), dbPath);
        FileInputFormat.addInputPath(mergeJob, tempDir);
        mergeJob.setReducerClass(StatusUpdateReducer.class);

        try {
            mergeJob.waitForCompletion(true);
        } catch (final Exception e) {
            LOG.error("DeduplicationMergeJob: {}", StringUtils.stringifyException(e));
            return -1;
        }

        CrawlDb.install(mergeJob, dbPath);

        // clean up
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(tempDir, true);

        long end = System.currentTimeMillis();
        LOG.info("Deduplication finished at {}, elapsed: ",
                sdf.format(end),
                TimingUtil.elapsedTime(start, end));

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(NutchConfiguration.create(),
                new DeduplicationJob(), args);
        System.exit(result);
    }

    @Override
    public Map<String, Object> run(Map<String, String> args, String crawlId) throws Exception {
//    if(args.size()<1){
//      throw new IllegalArgumentException("Required argument <crawldb>");
//    }
        Map<String, Object> results = new HashMap<String, Object>();
        String RESULT = "result";
        String[] arg = new String[1];
        String crawldb = crawlId+"/crawldb";
        arg[0] = crawldb;
        int res = run(arg);
        results.put(RESULT, Integer.toString(res));
        return results;
    }
}
