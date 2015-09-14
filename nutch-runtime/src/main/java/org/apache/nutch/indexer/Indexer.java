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

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.LinkDb;
import org.apache.nutch.io.Content;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.io.NutchWritable;
import org.apache.nutch.io.ParseText;
import org.apache.nutch.lib.output.IndexerOutputFormat;
import org.apache.nutch.mapper.IndexerMapper;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.reducer.IndexerReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class Indexer extends Configured {

    public static final Logger LOG = LoggerFactory
            .getLogger(Indexer.class);

    public static final String INDEXER_PARAMS = "indexer.additional.params";
    public static final String INDEXER_DELETE = "indexer.delete";
    public static final String URL_FILTERING = "indexer.url.filters";
    public static final String URL_NORMALIZING = "indexer.url.normalizers";
    public static final String INDEXER_BINARY_AS_BASE64 = "indexer.binary.base64";

    public static void initMRJob(Path crawlDb, Path linkDb,
                                 Collection<Path> segments, Job job, boolean addBinaryContent) throws IOException {

        LOG.info("Indexer: crawldb: {}", crawlDb);

        if (linkDb != null)
            LOG.info("Indexer: linkdb: {}", linkDb);

        for (final Path segment : segments) {
            LOG.info("IndexerMapReduces: adding segment: {}", segment);
            FileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.FETCH_DIR_NAME));
            FileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.PARSE_DIR_NAME));
            FileInputFormat.addInputPath(job, new Path(segment, ParseData.DIR_NAME));
            FileInputFormat.addInputPath(job, new Path(segment, ParseText.DIR_NAME));

            if (addBinaryContent) {
                FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));
            }
        }

        FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));

        if (linkDb != null) {
            Path currentLinkDb = new Path(linkDb, LinkDb.CURRENT_NAME);
            try {
                if (FileSystem.get(job.getConfiguration()).exists(currentLinkDb)) {
                    FileInputFormat.addInputPath(job, currentLinkDb);
                } else {
                    LOG.warn("Ignoring linkDb for indexing, no linkDb found in path: {}",
                            linkDb);
                }
            } catch (IOException e) {
                LOG.warn("Failed to use linkDb ({}) for indexing: {}", linkDb,
                        org.apache.hadoop.util.StringUtils.stringifyException(e));
            }
        }

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(IndexerMapper.class);
        job.setReducerClass(IndexerReducer.class);

        job.setOutputFormatClass(IndexerOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NutchWritable.class);
        job.setOutputValueClass(NutchWritable.class);
    }
}
