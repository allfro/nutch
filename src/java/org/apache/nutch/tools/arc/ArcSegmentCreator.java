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
package org.apache.nutch.tools.arc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.fetcher.FetcherOutputFormat;
import org.apache.nutch.mapper.ArcSegmentCreatorMapper;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <p>
 * The <code>ArcSegmentCreator</code> is a replacement for fetcher that will
 * take arc files as input and produce a nutch segment as output.
 * </p>
 *
 * <p>
 * Arc files are tars of compressed gzips which are produced by both the
 * internet archive project and the grub distributed crawler project.
 * </p>
 *
 */
public class ArcSegmentCreator extends Configured implements Tool {

    public static final Logger LOG = LoggerFactory
            .getLogger(ArcSegmentCreator.class);
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public ArcSegmentCreator() {

    }

    /**
     * <p>
     * Constructor that sets the job configuration.
     * </p>
     *
     * @param conf
     */
    public ArcSegmentCreator(Configuration conf) {
        setConf(conf);
    }

    /**
     * Generates a random name for the segments.
     *
     * @return The generated segment name.
     */
    public static synchronized String generateSegmentName() {
        try {
            Thread.sleep(1000);
        } catch (Throwable t) {
        }
        return sdf.format(new Date(System.currentTimeMillis()));
    }


    /**
     * <p>
     * Creates the arc files to segments job.
     * </p>
     *
     * @param arcFiles
     *          The path to the directory holding the arc files
     * @param segmentsOutDir
     *          The output directory for writing the segments
     *
     * @throws IOException
     *           If an IO error occurs while running the job.
     */
    public void createSegments(Path arcFiles, Path segmentsOutDir)
            throws IOException, ClassNotFoundException, InterruptedException {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        if (LOG.isInfoEnabled()) {
            LOG.info("ArcSegmentCreator: starting at " + sdf.format(start));
            LOG.info("ArcSegmentCreator: arc files dir: " + arcFiles);
        }

        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration, "ArcSegmentCreator " + arcFiles);
        String segName = generateSegmentName();
        configuration.set(Nutch.SEGMENT_NAME_KEY, segName);
        FileInputFormat.addInputPath(job, arcFiles);
        job.setInputFormatClass(ArcInputFormat.class);
        job.setMapperClass(ArcSegmentCreatorMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(segmentsOutDir, segName));
        job.setOutputFormatClass(FetcherOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NutchWritable.class);

        job.waitForCompletion(true);

        long end = System.currentTimeMillis();
        LOG.info("ArcSegmentCreator: finished at " + sdf.format(end)
                + ", elapsed: " + TimingUtil.elapsedTime(start, end));
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(),
                new ArcSegmentCreator(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        String usage = "Usage: ArcSegmentCreator <arcFiles> <segmentsOutDir>";

        if (args.length < 2) {
            System.err.println(usage);
            return -1;
        }

        // set the arc files directory and the segments output directory
        Path arcFiles = new Path(args[0]);
        Path segmentsOutDir = new Path(args[1]);

        try {
            // create the segments from the arc files
            createSegments(arcFiles, segmentsOutDir);
            return 0;
        } catch (Exception e) {
            LOG.error("ArcSegmentCreator: " + StringUtils.stringifyException(e));
            return -1;
        }
    }
}
