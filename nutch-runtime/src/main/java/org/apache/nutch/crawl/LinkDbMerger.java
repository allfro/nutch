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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.io.Inlinks;
import org.apache.nutch.mapper.LinkDbFilterMapper;
import org.apache.nutch.mapper.LinkDbMergerReducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Random;

/**
 * This tool merges several LinkDb-s into one, optionally filtering URLs through
 * the current URLFilters, to skip prohibited URLs and links.
 *
 * <p>
 * It's possible to use this tool just for filtering - in that case only one
 * LinkDb should be specified in arguments.
 * </p>
 * <p>
 * If more than one LinkDb contains information about the same URL, all inlinks
 * are accumulated, but only at most <code>db.max.inlinks</code> inlinks will
 * ever be added.
 * </p>
 * <p>
 * If activated, URLFilters will be applied to both the target URLs and to any
 * incoming link URL. If a target URL is prohibited, all inlinks to that target
 * will be removed, including the target URL. If some of incoming links are
 * prohibited, only they will be removed, and they won't count when checking the
 * above-mentioned maximum limit.
 *
 * @author Andrzej Bialecki
 */
public class LinkDbMerger extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(LinkDbMerger.class);

    public LinkDbMerger() {

    }

    public LinkDbMerger(Configuration conf) {
        setConf(conf);
    }

    public void merge(Path output, Path[] dbs, boolean normalize, boolean filter)
            throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        LOG.info("LinkDb merge: starting at {}", sdf.format(start));

        Job job = createMergeJob(getConf(), output, normalize, filter);
        for (Path db : dbs) {
            FileInputFormat.addInputPath(job, new Path(db, LinkDb.CURRENT_NAME));
        }
        job.waitForCompletion(true);
        FileSystem fs = FileSystem.get(getConf());
        fs.mkdirs(output);
        fs.rename(FileOutputFormat.getOutputPath(job), new Path(output, LinkDb.CURRENT_NAME));

        long end = System.currentTimeMillis();
        LOG.info("LinkDb merge: finished at {}, elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));
    }

    public static Job createMergeJob(Configuration config, Path linkDb,
                                         boolean normalize, boolean filter) throws IOException {
        Path newLinkDb = new Path("linkdb-merge-"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        Configuration configuration = new NutchJob(config);
        Job job = Job.getInstance(configuration, "linkdb merge " + linkDb);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(LinkDbFilterMapper.class);
        configuration.setBoolean(LinkDbFilterMapper.URL_NORMALIZING, normalize);
        configuration.setBoolean(LinkDbFilterMapper.URL_FILTERING, filter);
        job.setReducerClass(LinkDbMergerReducer.class);

        FileOutputFormat.setOutputPath(job, newLinkDb);
        job.setOutputFormatClass(MapFileOutputFormat.class);
        configuration.setBoolean(MRJobConfig.MAP_OUTPUT_COMPRESS, true);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Inlinks.class);

        // https://issues.apache.org/jira/browse/NUTCH-1069
        configuration.setBoolean(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false);

        return job;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new LinkDbMerger(),
                args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err
                    .println("Usage: LinkDbMerger <output_linkdb> <linkdb1> [<linkdb2> <linkdb3> ...] [-normalize] [-filter]");
            System.err.println("\toutput_linkdb\toutput LinkDb");
            System.err
                    .println("\tlinkdb1 ...\tinput LinkDb-s (single input LinkDb is ok)");
            System.err
                    .println("\t-normalize\tuse URLNormalizer on both fromUrls and toUrls in linkdb(s) (usually not needed)");
            System.err
                    .println("\t-filter\tuse URLFilters on both fromUrls and toUrls in linkdb(s)");
            return -1;
        }
        Path output = new Path(args[0]);
        ArrayList<Path> dbs = new ArrayList<Path>();
        boolean normalize = false;
        boolean filter = false;
        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "-filter":
                    filter = true;
                    break;
                case "-normalize":
                    normalize = true;
                    break;
                default:
                    dbs.add(new Path(args[i]));
                    break;
            }
        }
        try {
            merge(output, dbs.toArray(new Path[dbs.size()]), normalize, filter);
            return 0;
        } catch (Exception e) {
            LOG.error("LinkDbMerger: {}", StringUtils.stringifyException(e));
            return -1;
        }
    }

}
