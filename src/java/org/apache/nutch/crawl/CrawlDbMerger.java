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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

// Commons Logging imports
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.nutch.mapper.CrawlDbFilterMapper;
import org.apache.nutch.mapper.MergerMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;

/**
 * This tool merges several CrawlDb-s into one, optionally filtering URLs
 * through the current URLFilters, to skip prohibited pages.
 *
 * <p>
 * It's possible to use this tool just for filtering - in that case only one
 * CrawlDb should be specified in arguments.
 * </p>
 * <p>
 * If more than one CrawlDb contains information about the same URL, only the
 * most recent version is retained, as determined by the value of
 * {@link org.apache.nutch.crawl.CrawlDatum#getFetchTime()}. However, all
 * metadata information from all versions is accumulated, with newer values
 * taking precedence over older values.
 *
 * @author Andrzej Bialecki
 */
public class CrawlDbMerger extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory
            .getLogger(CrawlDbMerger.class);

    public CrawlDbMerger() {

    }

    public CrawlDbMerger(Configuration conf) {
        setConf(conf);
    }

    public void merge(Path output, Path[] dbs, boolean normalize, boolean filter)
            throws Exception {
        Configuration configuration = getConf();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("CrawlDb merge: starting at {}", sdf.format(start));

        Job job = createMergeJob(configuration, output, normalize, filter);
        for (Path db : dbs) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Adding {}", db);
            }
            FileInputFormat.addInputPath(job, new Path(db, CrawlDb.CURRENT_NAME));
        }
        job.waitForCompletion(true);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(output))
            fs.delete(output, true);
        fs.mkdirs(output);
        fs.rename(FileOutputFormat.getOutputPath(job), new Path(output,
                CrawlDb.CURRENT_NAME));
        long end = System.currentTimeMillis();
        LOG.info("CrawlDb merge: finished at {}, elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));
    }

    public static Job createMergeJob(Configuration configuration, Path output,
                                     boolean normalize, boolean filter) throws IOException {
        Path newCrawlDb = new Path("crawldb-merge-"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        Job job = Job.getInstance(configuration, "crawldb merge " + output);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(CrawlDbFilterMapper.class);
        configuration.setBoolean(CrawlDbFilterMapper.URL_FILTERING, filter);
        configuration.setBoolean(CrawlDbFilterMapper.URL_NORMALIZING, normalize);
        job.setReducerClass(MergerMapper.class);

        FileOutputFormat.setOutputPath(job, newCrawlDb);
        job.setOutputFormatClass(MapFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CrawlDatum.class);

        return job;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new CrawlDbMerger(),
                args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err
                    .println("Usage: CrawlDbMerger <output_crawldb> <crawldb1> [<crawldb2> <crawldb3> ...] [-normalize] [-filter]");
            System.err.println("\toutput_crawldb\toutput CrawlDb");
            System.err
                    .println("\tcrawldb1 ...\tinput CrawlDb-s (single input CrawlDb is ok)");
            System.err
                    .println("\t-normalize\tuse URLNormalizer on urls in the crawldb(s) (usually not needed)");
            System.err.println("\t-filter\tuse URLFilters on urls in the crawldb(s)");
            return -1;
        }
        Path output = new Path(args[0]);
        ArrayList<Path> dbs = new ArrayList<>();
        boolean filter = false;
        boolean normalize = false;
        FileSystem fs = FileSystem.get(getConf());
        for (int i = 1; i < args.length; i++) {
            if (args[i].equals("-filter")) {
                filter = true;
                continue;
            } else if (args[i].equals("-normalize")) {
                normalize = true;
                continue;
            }
            final Path dbPath = new Path(args[i]);
            if (fs.exists(dbPath))
                dbs.add(dbPath);
        }
        try {
            merge(output, dbs.toArray(new Path[dbs.size()]), normalize, filter);
            return 0;
        } catch (Exception e) {
            LOG.error("CrawlDb merge: {}", StringUtils.stringifyException(e));
            return -1;
        }
    }
}
