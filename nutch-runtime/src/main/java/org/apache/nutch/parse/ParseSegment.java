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

package org.apache.nutch.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.io.Content;
import org.apache.nutch.lib.output.ParseOutputFormat;
import org.apache.nutch.mapper.ParseSegmentMapper;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.reducer.ParseSegmentReducer;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.segment.SegmentChecker;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/* Parse content in a segment. */
public class ParseSegment extends NutchTool implements Tool {

    public static final Logger LOG = LoggerFactory.getLogger(ParseSegment.class);

    public static final String SKIP_TRUNCATED = "parser.skip.truncated";

    private ScoringFilters scfilters;

    private ParseUtil parseUtil;

    private boolean skipTruncated;

    public ParseSegment() {
        this(null);
    }

    public ParseSegment(Configuration conf) {
        super(conf);
    }



    public void parse(Path segment) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = getConf();
        if (SegmentChecker.isParsed(segment, FileSystem.get(configuration))) {
            LOG.warn("Segment: {} already parsed!! Skipped parsing this segment!!", segment); // NUTCH-1854
            return;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        if (LOG.isInfoEnabled()) {
            LOG.info("ParseSegment: starting at {}", sdf.format(start));
            LOG.info("ParseSegment: segment: {}", segment);
        }

        Job job = Job.getInstance(configuration, "parse " + segment);

        FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));
        configuration.set(Nutch.SEGMENT_NAME_KEY, segment.getName());
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(ParseSegmentMapper.class);
        job.setReducerClass(ParseSegmentReducer.class);

        FileOutputFormat.setOutputPath(job, segment);
        job.setOutputFormatClass(ParseOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ParseImpl.class);

        job.waitForCompletion(true);

        long end = System.currentTimeMillis();
        LOG.info("ParseSegment: finished at {} elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new ParseSegment(),
                args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Path segment;

        String usage = "Usage: ParseSegment segment [-noFilter] [-noNormalize]";

        if (args.length == 0) {
            System.err.println(usage);
            System.exit(-1);
        }

        if (args.length > 1) {
            Configuration configuration = getConf();
            for (int i = 1; i < args.length; i++) {
                String param = args[i];
                if ("-nofilter".equalsIgnoreCase(param)) {
                    configuration.setBoolean("parse.filter.urls", false);
                } else if ("-nonormalize".equalsIgnoreCase(param)) {
                    configuration.setBoolean("parse.normalize.urls", false);
                }
            }
        }

        segment = new Path(args[0]);
        parse(segment);
        return 0;
    }

    /*
     * Used for Nutch REST service
     */
    public Map<String, Object> run(Map<String, String> args, String crawlId) throws Exception {

        Map<String, Object> results = new HashMap<String, Object>();
        String RESULT = "result";
        if (args.containsKey("nofilter")) {
            getConf().setBoolean("parse.filter.urls", false);
        }
        if (args.containsKey("nonormalize")) {
            getConf().setBoolean("parse.normalize.urls", false);
        }

        String segment_dir = crawlId+"/segments";
        File segmentsDir = new File(segment_dir);
        File[] segmentsList = segmentsDir.listFiles();
        Arrays.sort(segmentsList, (f1, f2) -> (f1.lastModified()>f2.lastModified())?-1:0);

        Path segment = new Path(segmentsList[0].getPath());
        parse(segment);
        results.put(RESULT, Integer.toString(0));
        return results;
    }
}
