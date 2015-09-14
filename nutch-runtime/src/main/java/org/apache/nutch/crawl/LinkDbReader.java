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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.io.Inlink;
import org.apache.nutch.io.Inlinks;
import org.apache.nutch.mapper.LinkDBDumpMapper;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TimingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;

// Commons Logging imports

/** . */
public class LinkDbReader extends Configured implements Tool, Closeable {
    public static final Logger LOG = LoggerFactory.getLogger(LinkDbReader.class);

    private static final Partitioner<WritableComparable, Writable> PARTITIONER = new HashPartitioner<>();

    private Path directory;
    private MapFile.Reader[] readers;

    public LinkDbReader() {

    }

    public LinkDbReader(Configuration conf, Path directory) throws Exception {
        setConf(conf);
        init(directory);
    }

    public void init(Path directory) throws Exception {
        this.directory = directory;
    }

    public Inlinks getInlinks(Text url) throws IOException {

        if (readers == null) {
            synchronized (this) {
                readers = MapFileOutputFormat.getReaders(new Path(directory, LinkDb.CURRENT_NAME), getConf());
            }
        }

        return (Inlinks) MapFileOutputFormat.getEntry(readers, PARTITIONER, url, new Inlinks());
    }

    public void close() throws IOException {
        if (readers != null) {
            for (MapFile.Reader reader : readers) {
                reader.close();
            }
        }
    }

    public void processDumpJob(String linkdb, String output, String regex) throws IOException, ClassNotFoundException, InterruptedException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long start = System.currentTimeMillis();
        if (LOG.isInfoEnabled()) {
            LOG.info("LinkDb dump: starting at {}", sdf.format(start));
            LOG.info("LinkDb dump: db: {}", linkdb);
        }
        Path outFolder = new Path(output);

        Configuration configuration = new NutchJob(getConf());
        Job job = Job.getInstance(configuration, "read " + linkdb);
        job.setJarByClass(LinkDbReader.class);

        if (regex != null) {
            configuration.set("linkdb.regex", regex);
            job.setMapperClass(LinkDBDumpMapper.class);
        }

        FileInputFormat.addInputPath(job, new Path(linkdb, LinkDb.CURRENT_NAME));
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileOutputFormat.setOutputPath(job, outFolder);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Inlinks.class);

        job.waitForCompletion(true);

        long end = System.currentTimeMillis();
        LOG.info("LinkDb dump: finished at {}, elapsed: {}", sdf.format(end), TimingUtil.elapsedTime(start, end));
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(NutchConfiguration.create(), new LinkDbReader(),
                args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err
                    .println("Usage: LinkDbReader <linkdb> (-dump <out_dir> [-regex <regex>]) | -url <url>");
            System.err
                    .println("\t-dump <out_dir>\tdump whole link db to a text file in <out_dir>");
            System.err
                    .println("\t\t-regex <regex>\trestrict to url's matching expression");
            System.err
                    .println("\t-url <url>\tprint information about <url> to System.out");
            return -1;
        }
        try {
            switch (args[1]) {
                case "-dump":
                    String regex = null;
                    for (int i = 2; i < args.length; i++) {
                        if (args[i].equals("-regex")) {
                            regex = args[++i];
                        }
                    }
                    processDumpJob(args[0], args[2], regex);
                    return 0;
                case "-url":
                    init(new Path(args[0]));
                    Inlinks links = getInlinks(new Text(args[2]));
                    if (links == null) {
                        System.out.println(" - no link information.");
                    } else {
                        Iterator<Inlink> it = links.iterator();
                        while (it.hasNext()) {
                            System.out.println(it.next().toString());
                        }
                    }
                    return 0;
                default:
                    System.err.println("Error: wrong argument " + args[1]);
                    return -1;
            }
        } catch (Exception e) {
            LOG.error("LinkDbReader: {}", StringUtils.stringifyException(e));
            return -1;
        }
    }
}
