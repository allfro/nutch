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
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.lib.output.CrawlDatumCsvOutputFormat;
import org.apache.nutch.mapper.CrawlDbDumpMapper;
import org.apache.nutch.mapper.CrawlDbStatMapper;
import org.apache.nutch.mapper.CrawlDbTopNMapper;
import org.apache.nutch.reducer.CrawlDbStatCombiner;
import org.apache.nutch.reducer.CrawlDbStatReducer;
import org.apache.nutch.reducer.CrawlDbTopNReducer;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

// Commons Logging imports

/**
 * Read utility for the CrawlDB.
 *
 * @author Andrzej Bialecki
 *
 */
public class CrawlDbReader extends Configured implements Closeable, Tool {

    public static final Logger LOG = LoggerFactory.getLogger(CrawlDbReader.class);

    private MapFile.Reader[] readers = null;

    private void openReaders(String crawlDb, Configuration config)
            throws IOException {
        if (readers != null)
            return;
        FileSystem fs = FileSystem.get(config);
        readers = MapFileOutputFormat.getReaders(new Path(crawlDb, CrawlDb.CURRENT_NAME), config);
    }

    private void closeReaders() {
        if (readers == null)
            return;
        for (MapFile.Reader reader : readers) {
            try {
                reader.close();
            } catch (Exception ignored) {}
        }
    }

    public void close() {
        closeReaders();
    }

    private TreeMap<String, LongWritable> processStatJobHelper(String crawlDb, Configuration config, boolean sort)
            throws IOException, ClassNotFoundException, InterruptedException {
        Path tmpFolder = new Path(crawlDb, "stat_tmp" + System.currentTimeMillis());

        Configuration configuration = new NutchJob(config);
        Job job = Job.getInstance(configuration, "stats " + crawlDb);
        configuration.setBoolean("db.reader.stats.sort", sort);

        FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setMapperClass(CrawlDbStatMapper.class);
        job.setCombinerClass(CrawlDbStatCombiner.class);
        job.setReducerClass(CrawlDbStatReducer.class);

        FileOutputFormat.setOutputPath(job, tmpFolder);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // https://issues.apache.org/jira/browse/NUTCH-1029
        configuration.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

        job.waitForCompletion(true);

        // reading the result
        FileSystem fileSystem = FileSystem.get(config);
        MapFile.Reader[] readers = MapFileOutputFormat.getReaders(tmpFolder, configuration);

        Text key = new Text();
        LongWritable value = new LongWritable();

        TreeMap<String, LongWritable> stats = new TreeMap<String, LongWritable>();
        for (MapFile.Reader reader : readers) {
            while (reader.next(key, value)) {
                String k = key.toString();
                LongWritable val = stats.get(k);
                if (val == null) {
                    val = new LongWritable();
                    if (k.equals("scx"))
                        val.set(Long.MIN_VALUE);
                    if (k.equals("scn"))
                        val.set(Long.MAX_VALUE);
                    stats.put(k, val);
                }
                switch (k) {
                    case "scx":
                        if (val.get() < value.get())
                            val.set(value.get());
                        break;
                    case "scn":
                        if (val.get() > value.get())
                            val.set(value.get());
                        break;
                    default:
                        val.set(val.get() + value.get());
                        break;
                }
            }
            reader.close();
        }
        // removing the tmp folder
        fileSystem.delete(tmpFolder, true);
        return stats;
    }

    public void processStatJob(String crawlDb, Configuration config, boolean sort)
            throws IOException, InterruptedException, ClassNotFoundException {

        if (LOG.isInfoEnabled()) {
            LOG.info("CrawlDb statistics start: {}", crawlDb);
        }
        TreeMap<String, LongWritable> stats = processStatJobHelper(crawlDb, config, sort);

        if (LOG.isInfoEnabled()) {
            LOG.info("Statistics for CrawlDb: {}", crawlDb);
            LongWritable totalCnt = stats.get("T");
            stats.remove("T");
            LOG.info("TOTAL urls:\t{}", totalCnt.get());
            for (Map.Entry<String, LongWritable> entry : stats.entrySet()) {
                String k = entry.getKey();
                LongWritable val = entry.getValue();
                if (k.equals("scn")) {
                    LOG.info("min score:\t{}", (val.get() / 1000.0f));
                } else if (k.equals("scx")) {
                    LOG.info("max score:\t{}", (val.get() / 1000.0f));
                } else if (k.equals("sct")) {
                    LOG.info("avg score:\t{}",
                            (float) ((((double) val.get()) / totalCnt.get()) / 1000.0));
                } else if (k.startsWith("status")) {
                    String[] st = k.split(" ");
                    int code = Integer.parseInt(st[1]);
                    if (st.length > 2)
                        LOG.info("   {}:\t{}", st[2], val);
                    else
                        LOG.info("{} {} ({}):\t{}", st[0], code, CrawlDatum.getStatusName((byte) code), val);
                } else
                    LOG.info("{}:\t{}", k, val);
            }
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("CrawlDb statistics: done");
        }

    }

    public CrawlDatum get(String crawlDb, String url, Configuration config)
            throws IOException {
        Text key = new Text(url);
        CrawlDatum val = new CrawlDatum();
        openReaders(crawlDb, config);
        return (CrawlDatum) MapFileOutputFormat.getEntry(readers, new HashPartitioner<>(), key, val);
    }

    public void readUrl(String crawlDb, String url, Configuration config)
            throws IOException {
        CrawlDatum res = get(crawlDb, url, config);
        System.out.println("URL: " + url);
        if (res != null) {
            System.out.println(res);
        } else {
            System.out.println("not found");
        }
    }

    public void processDumpJob(String crawlDb, String output,
                               Configuration config, String format, String regex, String status,
                               Integer retry, String expr) throws IOException, ClassNotFoundException, InterruptedException {
        if (LOG.isInfoEnabled()) {
            LOG.info("CrawlDb dump: starting");
            LOG.info("CrawlDb db: {}", crawlDb);
        }

        Path outFolder = new Path(output);

        Configuration configuration = new NutchJob(config);
        Job job = Job.getInstance(configuration, "dump " + crawlDb);

        FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileOutputFormat.setOutputPath(job, outFolder);

        switch (format) {
            case "csv":
                job.setOutputFormatClass(CrawlDatumCsvOutputFormat.class);
                break;
            case "crawldb":
                job.setOutputFormatClass(MapFileOutputFormat.class);
                break;
            default:
                job.setOutputFormatClass(TextOutputFormat.class);
                break;
        }

        if (status != null)
            configuration.set("status", status);
        if (regex != null)
            configuration.set("regex", regex);
        if (retry != null)
            configuration.setInt("retry", retry);
        if (expr != null)
            configuration.set("expr", expr);

        job.setMapperClass(CrawlDbDumpMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CrawlDatum.class);

        job.waitForCompletion(true);
        if (LOG.isInfoEnabled()) {
            LOG.info("CrawlDb dump: done");
        }
    }

    public void processTopNJob(String crawlDb, long topN, float min, String output, Configuration config)
            throws IOException, ClassNotFoundException, InterruptedException {

        if (LOG.isInfoEnabled()) {
            LOG.info("CrawlDb topN: starting (topN={}, min={})", topN, min);
            LOG.info("CrawlDb db: {}", crawlDb);
        }

        Path outFolder = new Path(output);
        Path tempDir = new Path(config.get("mapred.temp.dir", ".")
                + "/readdb-topN-temp-"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

        Configuration configuration = new NutchJob(config);
        Job job = Job.getInstance(configuration, "topN prepare " + crawlDb);
        FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(CrawlDbTopNMapper.class);
        job.setReducerClass(Reducer.class);

        FileOutputFormat.setOutputPath(job, tempDir);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);

        configuration.setFloat("db.reader.topn.min", min);
        job.waitForCompletion(true);

        if (LOG.isInfoEnabled()) {
            LOG.info("CrawlDb topN: collecting topN scores.");
        }
        configuration = new NutchJob(config);
        job.setJobName("topN collect " + crawlDb);
        configuration.setLong("db.reader.topn", topN);

        FileInputFormat.addInputPath(job, tempDir);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(CrawlDbTopNReducer.class);

        FileOutputFormat.setOutputPath(job, outFolder);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1); // create a single file.

        job.waitForCompletion(true);
        FileSystem fs = FileSystem.get(config);
        fs.delete(tempDir, true);
        if (LOG.isInfoEnabled()) {
            LOG.info("CrawlDb topN: done");
        }

    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        @SuppressWarnings("resource")
        CrawlDbReader dbr = new CrawlDbReader();

        if (args.length < 2) {
            System.err
                    .println("Usage: CrawlDbReader <crawldb> (-stats | -dump <out_dir> | -topN <nnnn> <out_dir> [<min>] | -url <url>)");
            System.err
                    .println("\t<crawldb>\tdirectory name where crawldb is located");
            System.err
                    .println("\t-stats [-sort] \tprint overall statistics to System.out");
            System.err.println("\t\t[-sort]\tlist status sorted by host");
            System.err
                    .println("\t-dump <out_dir> [-format normal|csv|crawldb]\tdump the whole db to a text file in <out_dir>");
            System.err.println("\t\t[-format csv]\tdump in Csv format");
            System.err
                    .println("\t\t[-format normal]\tdump in standard format (default option)");
            System.err.println("\t\t[-format crawldb]\tdump as CrawlDB");
            System.err.println("\t\t[-regex <expr>]\tfilter records with expression");
            System.err.println("\t\t[-retry <num>]\tminimum retry count");
            System.err
                    .println("\t\t[-status <status>]\tfilter records by CrawlDatum status");
            System.err.println("\t\t[-expr <expr>]\tJexl expression to evaluate for this record");
            System.err
                    .println("\t-url <url>\tprint information on <url> to System.out");
            System.err
                    .println("\t-topN <nnnn> <out_dir> [<min>]\tdump top <nnnn> urls sorted by score to <out_dir>");
            System.err
                    .println("\t\t[<min>]\tskip records with scores below this value.");
            System.err.println("\t\t\tThis can significantly improve performance.");
            return -1;
        }
        String param;
        String crawlDb = args[0];
        Configuration configuration = new NutchJob(getConf());
        Job job = Job.getInstance(configuration);

        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "-stats":
                    boolean toSort = false;
                    if (i < args.length - 1 && "-sort".equals(args[i + 1])) {
                        toSort = true;
                        i++;
                    }
                    dbr.processStatJob(crawlDb, configuration, toSort);
                    break;
                case "-dump":
                    param = args[++i];
                    String format = "normal";
                    String regex = null;
                    Integer retry = null;
                    String status = null;
                    String expr = null;
                    for (int j = i + 1; j < args.length; j++) {
                        if (args[j].equals("-format")) {
                            format = args[++j];
                            i = i + 2;
                        }
                        if (args[j].equals("-regex")) {
                            regex = args[++j];
                            i = i + 2;
                        }
                        if (args[j].equals("-retry")) {
                            retry = Integer.parseInt(args[++j]);
                            i = i + 2;
                        }
                        if (args[j].equals("-status")) {
                            status = args[++j];
                            i = i + 2;
                        }
                        if (args[j].equals("-expr")) {
                            expr = args[++j];
                            i = i + 2;
                        }
                    }
                    dbr.processDumpJob(crawlDb, param, configuration, format, regex, status, retry, expr);
                    break;
                case "-url":
                    param = args[++i];
                    dbr.readUrl(crawlDb, param, configuration);
                    break;
                case "-topN":
                    param = args[++i];
                    long topN = Long.parseLong(param);
                    param = args[++i];
                    float min = 0.0f;
                    if (i < args.length - 1) {
                        min = Float.parseFloat(args[++i]);
                    }
                    dbr.processTopNJob(crawlDb, topN, min, param, configuration);
                    break;
                default:
                    System.err.println("\nError: wrong argument " + args[i]);
                    return -1;
            }
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(NutchConfiguration.create(),
                new CrawlDbReader(), args);
        System.exit(result);
    }
    public Object query(Map<String, String> args, Configuration conf, String type, String crawlId) throws Exception {


        Map<String, Object> results = new HashMap<String, Object>();
        String crawlDb = crawlId + "/crawldb";

        if(type.equalsIgnoreCase("stats")){
            boolean sort = false;
            if(args.containsKey("sort")){
                if(args.get("sort").equalsIgnoreCase("true"))
                    sort = true;
            }
            TreeMap<String , LongWritable> stats = processStatJobHelper(crawlDb, NutchConfiguration.create(), sort);
            LongWritable totalCnt = stats.get("T");
            stats.remove("T");
            results.put("totalUrls", String.valueOf(totalCnt.get()));
            Map<String, Object> statusMap = new HashMap<String, Object>();

            for (Map.Entry<String, LongWritable> entry : stats.entrySet()) {
                String k = entry.getKey();
                LongWritable val = entry.getValue();
                if (k.equals("scn")) {

                    results.put("minScore", String.valueOf((val.get() / 1000.0f)));
                } else if (k.equals("scx")) {
                    results.put("maxScore", String.valueOf((val.get() / 1000.0f)));
                } else if (k.equals("sct")) {
                    results.put("avgScore", String.valueOf((float) ((((double) val.get()) / totalCnt.get()) / 1000.0)));
                } else if (k.startsWith("status")) {
                    String[] st = k.split(" ");
                    int code = Integer.parseInt(st[1]);
                    if (st.length > 2){
                        @SuppressWarnings("unchecked")
                        Map<String, Object> individualStatusInfo = (Map<String, Object>) statusMap.get(String.valueOf(code));
                        Map<String, String> hostValues;
                        if(individualStatusInfo.containsKey("hostValues")){
                            hostValues= (Map<String, String>) individualStatusInfo.get("hostValues");
                        }
                        else{
                            hostValues = new HashMap<String, String>();
                            individualStatusInfo.put("hostValues", hostValues);
                        }
                        hostValues.put(st[2], String.valueOf(val));
                    }
                    else{
                        Map<String, Object> individualStatusInfo = new HashMap<String, Object>();

                        individualStatusInfo.put("statusValue", CrawlDatum.getStatusName((byte) code));
                        individualStatusInfo.put("count", String.valueOf(val));

                        statusMap.put(String.valueOf(code), individualStatusInfo);
                    }
                } else
                    results.put(k, String.valueOf(val));
            }
            results.put("status", statusMap);
            return results;
        }
        if(type.equalsIgnoreCase("dump")){
            String output = args.get("out_dir");
            String format = "normal";
            String regex = null;
            Integer retry = null;
            String status = null;
            String expr = null;
            if (args.containsKey("format")) {
                format = args.get("format");
            }
            if (args.containsKey("regex")) {
                regex = args.get("regex");
            }
            if (args.containsKey("retry")) {
                retry = Integer.parseInt(args.get("retry"));
            }
            if (args.containsKey("status")) {
                status = args.get("status");
            }
            if (args.containsKey("expr")) {
                expr = args.get("expr");
            }
            processDumpJob(crawlDb, output, new NutchJob(conf), format, regex, status, retry, expr);
            File dumpFile = new File(output+"/part-00000");
            return dumpFile;
        }
        if (type.equalsIgnoreCase("topN")) {
            String output = args.get("out_dir");
            long topN = Long.parseLong(args.get("nnn"));
            float min = 0.0f;
            if(args.containsKey("min")){
                min = Float.parseFloat(args.get("min"));
            }
            processTopNJob(crawlDb, topN, min, output, new NutchJob(conf));
            File dumpFile = new File(output+"/part-00000");
            return dumpFile;
        }

        if(type.equalsIgnoreCase("url")){
            String url = args.get("url");
            CrawlDatum res = get(crawlDb, url, new NutchJob(conf));
            results.put("status", res.getStatus());
            results.put("fetchTime", new Date(res.getFetchTime()));
            results.put("modifiedTime", new Date(res.getModifiedTime()));
            results.put("retriesSinceFetch", res.getRetriesSinceFetch());
            results.put("retryInterval", res.getFetchInterval());
            results.put("score", res.getScore());
            results.put("signature", StringUtil.toHexString(res.getSignature()));
            Map<String, String> metadata = new HashMap<String, String>();
            if(res.getMetaData()!=null){
                for (Entry<Writable, Writable> e : res.getMetaData().entrySet()) {
                    metadata.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
                }
            }
            results.put("metadata", metadata);

            return results;
        }
        return results;
    }
}