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
package org.apache.nutch.indexer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.mapper.CleanerMapper;
import org.apache.nutch.reducer.DeleterReducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * The class scans CrawlDB looking for entries with status DB_GONE (404) or
 * DB_DUPLICATE and sends delete requests to indexers for those documents.
 */

public class CleaningJob implements Tool {
    public static final Logger LOG = LoggerFactory.getLogger(CleaningJob.class);
    private Configuration conf;

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public void delete(String crawldb, boolean noCommit) throws IOException, ClassNotFoundException, InterruptedException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("CleaningJob: starting at {}", sdf.format(start));

        Configuration configuration = new NutchJob(getConf());
        Job job = Job.getInstance(configuration, "CleaningJob");
        job.setJarByClass(CleaningJob.class);

        FileInputFormat.addInputPath(job, new Path(crawldb, CrawlDb.CURRENT_NAME));
        configuration.setBoolean("noCommit", noCommit);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapOutputKeyClass(ByteWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(CleanerMapper.class);
        job.setReducerClass(DeleterReducer.class);


        // need to expicitely allow deletions
        configuration.setBoolean(Indexer.INDEXER_DELETE, true);

        job.waitForCompletion(true);

        long end = System.currentTimeMillis();
        LOG.info("CleaningJob: finished at {}, elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));
    }

    public int run(String[] args) throws IOException {
        if (args.length < 1) {
            String usage = "Usage: CleaningJob <crawldb> [-noCommit]";
            LOG.error("Missing crawldb. {}", usage);
            System.err.println(usage);
            IndexWriters writers = new IndexWriters(getConf());
            System.err.println(writers.describe());
            return 1;
        }

        boolean noCommit = false;
        if (args.length == 2 && args[1].equals("-noCommit")) {
            noCommit = true;
        }

        try {
            delete(args[0], noCommit);
        } catch (final Exception e) {
            LOG.error("CleaningJob: {}", StringUtils.stringifyException(e));
            System.err.println("ERROR CleaningJob: "
                    + StringUtils.stringifyException(e));
            return -1;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(NutchConfiguration.create(), new CleaningJob(),
                args);
        System.exit(result);
    }
}
