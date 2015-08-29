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

package org.apache.nutch.crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.mapper.InjectorMapper;
import org.apache.nutch.reducer.InjectorReducer;
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

// Commons Logging imports

/**
 * This class takes a flat file of URLs and adds them to the of pages to be
 * crawled. Useful for bootstrapping the system. The URL files contain one URL
 * per line, optionally followed by custom metadata separated by tabs with the
 * metadata key separated from the corresponding value by '='. <br>
 * Note that some metadata keys are reserved : <br>
 * - <i>nutch.score</i> : allows to set a custom score for a specific URL <br>
 * - <i>nutch.fetchInterval</i> : allows to set a custom fetch interval for a
 * specific URL <br>
 * - <i>nutch.fetchInterval.fixed</i> : allows to set a custom fetch interval
 * for a specific URL that is not changed by AdaptiveFetchSchedule <br>
 * e.g. http://www.nutch.org/ \t nutch.score=10 \t nutch.fetchInterval=2592000
 * \t userType=open_source
 **/
public class Injector extends NutchTool implements Tool {
    public static final Logger LOG = LoggerFactory.getLogger(Injector.class);


    public Injector() {
    }

    public Injector(Configuration conf) {
        setConf(conf);
    }

    public void inject(Path crawlDbDir, Path urlSeedDir) throws IOException {

        Configuration configuration = getConf();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();

        if (LOG.isInfoEnabled()) {
            LOG.info("Injector: starting at {}", sdf.format(start));
            LOG.info("Injector: crawlDb: {}", crawlDbDir);
            LOG.info("Injector: urlDir: {}", urlSeedDir);
        }

        Path tempDir = new Path(configuration.get("mapred.temp.dir", ".")
                + "/inject-temp-"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        // map text input file to a <url,CrawlDatum> file
        if (LOG.isInfoEnabled()) {
            LOG.info("Injector: Converting injected urls to crawl db entries.");
        }

        FileSystem fs = FileSystem.get(configuration);
        // determine if the crawldb already exists
        boolean dbExists = fs.exists(crawlDbDir);

        Job sortJob = Job.getInstance(configuration, "inject " + urlSeedDir);
        FileInputFormat.addInputPath(sortJob, urlSeedDir);
        sortJob.setMapperClass(InjectorMapper.class);

        FileOutputFormat.setOutputPath(sortJob, tempDir);

        if (dbExists) {
            // Don't run merge injected urls, wait for merge with
            // existing DB
            sortJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            sortJob.setNumReduceTasks(0);
        } else {
            sortJob.setOutputFormatClass(MapFileOutputFormat.class);
            sortJob.setReducerClass(InjectorReducer.class);
            configuration.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
        }
        sortJob.setOutputKeyClass(Text.class);
        sortJob.setOutputValueClass(CrawlDatum.class);
        configuration.setLong("injector.current.time", System.currentTimeMillis());

        try {
            sortJob.waitForCompletion(true);
        } catch (InterruptedException | ClassNotFoundException | IOException e) {
            fs.delete(tempDir, true);
            throw new IOException(e);
        }

        long urlsInjected = sortJob.getCounters()
                .findCounter("injector", "urls_injected").getValue();
        long urlsFiltered = sortJob.getCounters()
                .findCounter("injector", "urls_filtered").getValue();

        LOG.info("Injector: Total number of urls rejected by filters: {}", urlsFiltered);
        LOG.info("Injector: Total number of urls after normalization: {}", urlsInjected);

        long urlsMerged = 0;

        if (dbExists) {
            // merge with existing crawl db
            if (LOG.isInfoEnabled()) {
                LOG.info("Injector: Merging injected urls into crawl db.");
            }
            Job mergeJob = CrawlDb.createJob(configuration, crawlDbDir);
            FileInputFormat.addInputPath(mergeJob, tempDir);
            mergeJob.setReducerClass(InjectorReducer.class);
            try {
                mergeJob.waitForCompletion(true);
                urlsMerged = mergeJob.getCounters().findCounter("injector", "urls_merged")
                        .getValue();
                LOG.info("Injector: URLs merged: {}", urlsMerged);
            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                fs.delete(tempDir, true);
                throw new IOException(e);
            }
            CrawlDb.install(mergeJob, crawlDbDir);
        } else {
            CrawlDb.install(sortJob, crawlDbDir);
        }

        // clean up
        fs.delete(tempDir, true);
        LOG.info("Injector: Total new urls injected: {}", urlsInjected - urlsMerged);

        long end = System.currentTimeMillis();
        LOG.info("Injector: finished at {}, elapsed: ",
                sdf.format(end),
                TimingUtil.elapsedTime(start, end)
        );
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new Injector(), args);
        System.exit(res);
    }

    /**
     * Main entry point to start job.
     * @param args command line arguments to pass to job
     * @return exit integer status.
     * @throws Exception when the job fails to run.
     */
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Injector <crawldb> <url_dir>");
            return -1;
        }
        try {
            inject(new Path(args[0]), new Path(args[1]));
            return 0;
        } catch (Exception e) {
            LOG.error("Injector: " + StringUtils.stringifyException(e));
            return -1;
        }
    }

    @Override
    /**
     * Used by the Nutch REST service
     */
    public Map<String, Object> run(Map<String, String> args, String crawlId) throws Exception {
        if(args.size()<1){
            throw new IllegalArgumentException("Required arguments <url_dir>");
        }
        Map<String, Object> results = new HashMap<String, Object>();
        String RESULT = "result";
        String crawldb = crawlId+"/crawldb";
        String url_dir = args.get("url_dir");

        inject(new Path(crawldb), new Path(url_dir));
        results.put(RESULT, Integer.toString(0));
        return results;

    }

}
