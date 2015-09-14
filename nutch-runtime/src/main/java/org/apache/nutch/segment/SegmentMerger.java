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
package org.apache.nutch.segment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.io.Content;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.io.ParseText;
import org.apache.nutch.lib.input.ObjectInputFormat;
import org.apache.nutch.lib.output.SegmentOutputFormat;
import org.apache.nutch.mapper.SegmentMergerMapper;
import org.apache.nutch.metadata.MetaWrapper;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.reducer.SegmentMergerReducer;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * This tool takes several segments and merges their data together. Only the
 * latest versions of data is retained.
 * <p>
 * Optionally, you can apply current URLFilters to remove prohibited URL-s.
 * </p>
 * <p>
 * Also, it's possible to slice the resulting segment into chunks of fixed size.
 * </p>
 * <h3>Important Notes</h3> <h4>Which parts are merged?</h4>
 * <p>
 * It doesn't make sense to merge data from segments, which are at different
 * stages of processing (e.g. one unfetched segment, one fetched but not parsed,
 * and one fetched and parsed). Therefore, prior to merging, the tool will
 * determine the lowest common set of input data, and only this data will be
 * merged. This may have some unintended consequences: e.g. if majority of input
 * segments are fetched and parsed, but one of them is unfetched, the tool will
 * fall back to just merging fetchlists, and it will skip all other data from
 * all segments.
 * </p>
 * <h4>Merging fetchlists</h4>
 * <p>
 * Merging segments, which contain just fetchlists (i.e. prior to fetching) is
 * not recommended, because this tool (unlike the
 * {@link org.apache.nutch.crawl.Generator} doesn't ensure that fetchlist parts
 * for each map task are disjoint.
 * </p>
 * <p>
 * <h4>Duplicate content</h4>
 * Merging segments removes older content whenever possible (see below).
 * However, this is NOT the same as de-duplication, which in addition removes
 * identical content found at different URL-s. In other words, running
 * DeleteDuplicates is still necessary.
 * </p>
 * <p>
 * For some types of data (especially ParseText) it's not possible to determine
 * which version is really older. Therefore the tool always uses segment names
 * as timestamps, for all types of input data. Segment names are compared in
 * forward lexicographic order (0-9a-zA-Z), and data from segments with "higher"
 * names will prevail. It follows then that it is extremely important that
 * segments be named in an increasing lexicographic order as their creation time
 * increases.
 * </p>
 * <p>
 * <h4>Merging and indexes</h4>
 * Merged segment gets a different name. Since Indexer embeds segment names in
 * indexes, any indexes originally created for the input segments will NOT work
 * with the merged segment. Newly created merged segment(s) need to be indexed
 * afresh. This tool doesn't use existing indexes in any way, so if you plan to
 * merge segments you don't have to index them prior to merging.
 *
 *
 * @author Andrzej Bialecki
 */
public class SegmentMerger extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory
            .getLogger(SegmentMerger.class);

    public static final String SEGMENT_PART_KEY = "part";
    public static final String SEGMENT_SLICE_KEY = "slice";


    public SegmentMerger() {
        super(null);
    }

    public SegmentMerger(Configuration conf) {
        super(conf);
    }


    public void merge(Path out, Path[] segs, boolean filter, boolean normalize,
                      long slice) throws Exception {
        String segmentName = Generator.generateSegmentName();
        if (LOG.isInfoEnabled()) {
            LOG.info("Merging " + segs.length + " segments to " + out + "/"
                    + segmentName);
        }
        Configuration configuration = new NutchJob(getConf());
        Job job = Job.getInstance(configuration, "mergesegs " + out + "/" + segmentName);
        configuration.setBoolean("segment.merger.filter", filter);
        configuration.setBoolean("segment.merger.normalizer", normalize);
        configuration.setLong("segment.merger.slice", slice);
        configuration.set("segment.merger.segmentName", segmentName);
        FileSystem fs = FileSystem.get(configuration);
        // prepare the minimal common set of input dirs
        boolean g = true;
        boolean f = true;
        boolean p = true;
        boolean c = true;
        boolean pd = true;
        boolean pt = true;
        for (int i = 0; i < segs.length; i++) {
            if (!fs.exists(segs[i])) {
                if (LOG.isWarnEnabled()) LOG.warn("Input dir {} doesn't exist, skipping.", segs[i]);
                segs[i] = null;
                continue;
            }
            if (LOG.isInfoEnabled()) {
                LOG.info("SegmentMerger:   adding {}", segs[i]);
            }
            Path cDir = new Path(segs[i], Content.DIR_NAME);
            Path gDir = new Path(segs[i], CrawlDatum.GENERATE_DIR_NAME);
            Path fDir = new Path(segs[i], CrawlDatum.FETCH_DIR_NAME);
            Path pDir = new Path(segs[i], CrawlDatum.PARSE_DIR_NAME);
            Path pdDir = new Path(segs[i], ParseData.DIR_NAME);
            Path ptDir = new Path(segs[i], ParseText.DIR_NAME);
            c = c && fs.exists(cDir);
            g = g && fs.exists(gDir);
            f = f && fs.exists(fDir);
            p = p && fs.exists(pDir);
            pd = pd && fs.exists(pdDir);
            pt = pt && fs.exists(ptDir);
        }
        StringBuilder sb = new StringBuilder();
        if (c)
            sb.append(" " + Content.DIR_NAME);
        if (g)
            sb.append(" " + CrawlDatum.GENERATE_DIR_NAME);
        if (f)
            sb.append(" " + CrawlDatum.FETCH_DIR_NAME);
        if (p)
            sb.append(" " + CrawlDatum.PARSE_DIR_NAME);
        if (pd)
            sb.append(" " + ParseData.DIR_NAME);
        if (pt)
            sb.append(" " + ParseText.DIR_NAME);
        if (LOG.isInfoEnabled()) {
            LOG.info("SegmentMerger: using segment data from: {}", sb.toString());
        }
        for (Path seg : segs) {
            if (seg == null)
                continue;
            if (g) {
                Path gDir = new Path(seg, CrawlDatum.GENERATE_DIR_NAME);
                FileInputFormat.addInputPath(job, gDir);
            }
            if (c) {
                Path cDir = new Path(seg, Content.DIR_NAME);
                FileInputFormat.addInputPath(job, cDir);
            }
            if (f) {
                Path fDir = new Path(seg, CrawlDatum.FETCH_DIR_NAME);
                FileInputFormat.addInputPath(job, fDir);
            }
            if (p) {
                Path pDir = new Path(seg, CrawlDatum.PARSE_DIR_NAME);
                FileInputFormat.addInputPath(job, pDir);
            }
            if (pd) {
                Path pdDir = new Path(seg, ParseData.DIR_NAME);
                FileInputFormat.addInputPath(job, pdDir);
            }
            if (pt) {
                Path ptDir = new Path(seg, ParseText.DIR_NAME);
                FileInputFormat.addInputPath(job, ptDir);
            }
        }
        job.setInputFormatClass(ObjectInputFormat.class);
        job.setMapperClass(SegmentMergerMapper.class);
        job.setReducerClass(SegmentMergerReducer.class);
        FileOutputFormat.setOutputPath(job, out);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MetaWrapper.class);
        job.setOutputFormatClass(SegmentOutputFormat.class);

        job.waitForCompletion(true);
    }

    /**
     * @param args
     */
    public int run(String[] args)  throws Exception {
        if (args.length < 2) {
            System.err
                    .println("SegmentMerger output_dir (-dir segments | seg1 seg2 ...) [-filter] [-slice NNNN]");
            System.err
                    .println("\toutput_dir\tname of the parent dir for output segment slice(s)");
            System.err
                    .println("\t-dir segments\tparent dir containing several segments");
            System.err.println("\tseg1 seg2 ...\tlist of segment dirs");
            System.err
                    .println("\t-filter\t\tfilter out URL-s prohibited by current URLFilters");
            System.err
                    .println("\t-normalize\t\tnormalize URL via current URLNormalizers");
            System.err
                    .println("\t-slice NNNN\tcreate many output segments, each containing NNNN URLs");
            return -1;
        }
        Configuration conf = NutchConfiguration.create();
        final FileSystem fs = FileSystem.get(conf);
        Path out = new Path(args[0]);
        ArrayList<Path> segs = new ArrayList<Path>();
        long sliceSize = 0;
        boolean filter = false;
        boolean normalize = false;
        for (int i = 1; i < args.length; i++) {
            if (args[i].equals("-dir")) {
                FileStatus[] fstats = fs.listStatus(new Path(args[++i]),
                        HadoopFSUtil.getPassDirectoriesFilter(fs));
                Path[] files = HadoopFSUtil.getPaths(fstats);
                for (int j = 0; j < files.length; j++)
                    segs.add(files[j]);
            } else if (args[i].equals("-filter")) {
                filter = true;
            } else if (args[i].equals("-normalize")) {
                normalize = true;
            } else if (args[i].equals("-slice")) {
                sliceSize = Long.parseLong(args[++i]);
            } else {
                segs.add(new Path(args[i]));
            }
        }
        if (segs.size() == 0) {
            System.err.println("ERROR: No input segments.");
            return -1;
        }

        merge(out, segs.toArray(new Path[segs.size()]), filter, normalize,
                sliceSize);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(NutchConfiguration.create(),
                new SegmentMerger(), args);
        System.exit(result);
    }

}