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
package org.apache.nutch.segment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.nutch.io.Content;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.io.NutchWritable;
import org.apache.nutch.io.ParseText;
import org.apache.nutch.lib.output.TextOutputFormat;
import org.apache.nutch.mapper.InputCompatMapper;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.reducer.SegmentReaderReducer;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/** Dump the content of a segment. */
public class SegmentReader extends Configured {

    public static final Logger LOG = LoggerFactory.getLogger(SegmentReader.class);



    private boolean co, fe, ge, pa, pd, pt;
    private FileSystem fs;

    public SegmentReader() {
        super(null);
    }

    public SegmentReader(Configuration conf, boolean co, boolean fe, boolean ge,
                         boolean pa, boolean pd, boolean pt) {
        super(conf);
        this.co = co;
        this.fe = fe;
        this.ge = ge;
        this.pa = pa;
        this.pd = pd;
        this.pt = pt;
        try {
            this.fs = FileSystem.get(getConf());
        } catch (IOException e) {
            LOG.error("IOException:", e);
        }
    }


    private Configuration createJobConf() {
        Configuration configuration = new NutchJob(getConf());
        configuration.setBoolean("segment.reader.co", this.co);
        configuration.setBoolean("segment.reader.fe", this.fe);
        configuration.setBoolean("segment.reader.ge", this.ge);
        configuration.setBoolean("segment.reader.pa", this.pa);
        configuration.setBoolean("segment.reader.pd", this.pd);
        configuration.setBoolean("segment.reader.pt", this.pt);
        return configuration;
    }

    public void dump(Path segment, Path output) throws IOException, ClassNotFoundException, InterruptedException {

        if (LOG.isInfoEnabled()) {
            LOG.info("SegmentReader: dump segment: {}", segment);
        }

        Configuration configuration = createJobConf();
        Job job = Job.getInstance(configuration, "read " + segment);

        if (ge)
            FileInputFormat.addInputPath(job, new Path(segment,
                    CrawlDatum.GENERATE_DIR_NAME));
        if (fe)
            FileInputFormat.addInputPath(job, new Path(segment,
                    CrawlDatum.FETCH_DIR_NAME));
        if (pa)
            FileInputFormat.addInputPath(job, new Path(segment,
                    CrawlDatum.PARSE_DIR_NAME));
        if (co)
            FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));
        if (pd)
            FileInputFormat.addInputPath(job, new Path(segment, ParseData.DIR_NAME));
        if (pt)
            FileInputFormat.addInputPath(job, new Path(segment, ParseText.DIR_NAME));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(InputCompatMapper.class);
        job.setReducerClass(SegmentReaderReducer.class);

        Path tempDir = new Path(configuration.get("hadoop.tmp.dir", "/tmp") + "/segread-"
                + new java.util.Random().nextInt());
        fs.delete(tempDir, true);

        FileOutputFormat.setOutputPath(job, tempDir);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NutchWritable.class);

        job.waitForCompletion(true);

        // concatenate the output
        Path dumpFile = new Path(output, configuration.get("segment.dump.dir", "dump"));

        // remove the old file
        fs.delete(dumpFile, true);
        FileStatus[] fstats = fs.listStatus(tempDir,
                HadoopFSUtil.getPassAllFilter());
        Path[] files = HadoopFSUtil.getPaths(fstats);

        PrintWriter writer;
        int currentRecordNumber = 0;
        if (files.length > 0) {
            writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
                    fs.create(dumpFile))));
            try {
                for (Path partFile : files) {
                    try {
                        currentRecordNumber = append(fs, configuration, partFile, writer,
                                currentRecordNumber);
                    } catch (IOException exception) {
                        if (LOG.isWarnEnabled()) {
                            LOG.warn("Couldn't copy the content of " + partFile.toString()
                                    + " into " + dumpFile.toString());
                            LOG.warn(exception.getMessage());
                        }
                    }
                }
            } finally {
                writer.close();
            }
        }
        fs.delete(tempDir, true);
        if (LOG.isInfoEnabled()) {
            LOG.info("SegmentReader: done");
        }
    }

    /** Appends two files and updates the Recno counter */
    private int append(FileSystem fs, Configuration conf, Path src,
                       PrintWriter writer, int currentRecordNumber) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                fs.open(src)));
        try {
            String line = reader.readLine();
            while (line != null) {
                if (line.startsWith("Recno:: ")) {
                    line = "Recno:: " + currentRecordNumber++;
                }
                writer.println(line);
                line = reader.readLine();
            }
            return currentRecordNumber;
        } finally {
            reader.close();
        }
    }

    private static final String[][] keys = new String[][] {
            { "co", "Content::\n" }, { "ge", "Crawl Generate::\n" },
            { "fe", "Crawl Fetch::\n" }, { "pa", "Crawl Parse::\n" },
            { "pd", "ParseData::\n" }, { "pt", "ParseText::\n" } };

    public void get(final Path segment, final Text key, Writer writer,
                    final Map<String, List<Writable>> results) throws Exception {
        LOG.info("SegmentReader: get '{}'", key);
        ArrayList<Thread> threads = new ArrayList<>();
        if (co)
            threads.add(new Thread() {
                public void run() {
                    try {
                        List<Writable> res = getMapRecords(new Path(segment,
                                Content.DIR_NAME), key);
                        results.put("co", res);
                    } catch (Exception e) {
                        LOG.error("Exception:", e);
                    }
                }
            });
        if (fe)
            threads.add(new Thread() {
                public void run() {
                    try {
                        List<Writable> res = getMapRecords(new Path(segment,
                                CrawlDatum.FETCH_DIR_NAME), key);
                        results.put("fe", res);
                    } catch (Exception e) {
                        LOG.error("Exception:", e);
                    }
                }
            });
        if (ge)
            threads.add(new Thread() {
                public void run() {
                    try {
                        List<Writable> res = getSeqRecords(new Path(segment,
                                CrawlDatum.GENERATE_DIR_NAME), key);
                        results.put("ge", res);
                    } catch (Exception e) {
                        LOG.error("Exception:", e);
                    }
                }
            });
        if (pa)
            threads.add(new Thread() {
                public void run() {
                    try {
                        List<Writable> res = getSeqRecords(new Path(segment,
                                CrawlDatum.PARSE_DIR_NAME), key);
                        results.put("pa", res);
                    } catch (Exception e) {
                        LOG.error("Exception:", e);
                    }
                }
            });
        if (pd)
            threads.add(new Thread() {
                public void run() {
                    try {
                        List<Writable> res = getMapRecords(new Path(segment,
                                ParseData.DIR_NAME), key);
                        results.put("pd", res);
                    } catch (Exception e) {
                        LOG.error("Exception:", e);
                    }
                }
            });
        if (pt)
            threads.add(new Thread() {
                public void run() {
                    try {
                        List<Writable> res = getMapRecords(new Path(segment,
                                ParseText.DIR_NAME), key);
                        results.put("pt", res);
                    } catch (Exception e) {
                        LOG.error("Exception:", e);
                    }
                }
            });
        threads.forEach(java.lang.Thread::start);
        int cnt;
        do {
            cnt = 0;
            try {
                Thread.sleep(5000);
            } catch (Exception ignored) {
            }

            for (Thread thread : threads) {
                if (thread.isAlive())
                    cnt++;
            }
            if ((cnt > 0) && (LOG.isDebugEnabled())) {
                LOG.debug("({} to retrieve)", cnt);
            }
        } while (cnt > 0);
        for (String[] key1 : keys) {
            List<Writable> res = results.get(key1[0]);
            if (res != null && res.size() > 0) {
                for (Writable re : res) {
                    writer.write(key1[1]);
                    writer.write(re + "\n");
                }
            }
            writer.flush();
        }
    }

    private List<Writable> getMapRecords(Path dir, Text key) throws Exception {
        MapFile.Reader[] readers = MapFileOutputFormat.getReaders(dir, getConf());
        ArrayList<Writable> res = new ArrayList<>();
        Class<?> keyClass = readers[0].getKeyClass();
        Class<?> valueClass = readers[0].getValueClass();
        if (!keyClass.getName().equals("org.apache.hadoop.io.Text"))
            throw new IOException("Incompatible key (" + keyClass.getName() + ")");
        Writable value = (Writable) valueClass.newInstance();
        // we don't know the partitioning schema
        for (MapFile.Reader reader : readers) {
            if (reader.get(key, value) != null) {
                res.add(value);
                value = (Writable) valueClass.newInstance();
                Text aKey = (Text) keyClass.newInstance();
                while (reader.next(aKey, value) && aKey.equals(key)) {
                    res.add(value);
                    value = (Writable) valueClass.newInstance();
                }
            }
            reader.close();
        }
        return res;
    }

    private List<Writable> getSeqRecords(Path dir, Text key) throws Exception {
        MapFile.Reader[] readers = MapFileOutputFormat.getReaders(dir, getConf());
        ArrayList<Writable> res = new ArrayList<>();
        Class<?> keyClass = readers[0].getKeyClass();
        Class<?> valueClass = readers[0].getValueClass();
        if (!keyClass.getName().equals("org.apache.hadoop.io.Text"))
            throw new IOException("Incompatible key (" + keyClass.getName() + ")");
        Writable aKey = (Writable) keyClass.newInstance();
        Writable value = (Writable) valueClass.newInstance();
        for (MapFile.Reader reader : readers) {
            while (reader.next((WritableComparable) aKey, value)) {
                if (aKey.equals(key)) {
                    res.add(value);
                    value = (Writable) valueClass.newInstance();
                }
            }
            reader.close();
        }
        return res;
    }

    public static class SegmentReaderStats {
        public long start = -1L;
        public long end = -1L;
        public long generated = -1L;
        public long fetched = -1L;
        public long parsed = -1L;
        public long parseErrors = -1L;
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    public void list(List<Path> dirs, Writer writer) throws Exception {
        writer
                .write("NAME\t\tGENERATED\tFETCHER START\t\tFETCHER END\t\tFETCHED\tPARSED\n");
        for (Path dir : dirs) {
            SegmentReaderStats stats = new SegmentReaderStats();
            getStats(dir, stats);
            writer.write(dir.getName() + "\t");
            if (stats.generated == -1)
                writer.write("?");
            else
                writer.write(stats.generated + "");
            writer.write("\t\t");
            if (stats.start == -1)
                writer.write("?\t");
            else
                writer.write(sdf.format(new Date(stats.start)));
            writer.write("\t");
            if (stats.end == -1)
                writer.write("?");
            else
                writer.write(sdf.format(new Date(stats.end)));
            writer.write("\t");
            if (stats.fetched == -1)
                writer.write("?");
            else
                writer.write(stats.fetched + "");
            writer.write("\t");
            if (stats.parsed == -1)
                writer.write("?");
            else
                writer.write(stats.parsed + "");
            writer.write("\n");
            writer.flush();
        }
    }

    public void getStats(Path segment, final SegmentReaderStats stats)
            throws Exception {
        long cnt = 0L;
        Text key = new Text();

        if (ge) {
            MapFile.Reader[] readers = MapFileOutputFormat.getReaders(
                    new Path(segment, CrawlDatum.GENERATE_DIR_NAME), getConf());
            for (MapFile.Reader reader : readers) {
                while (reader.next(key, null))
                    cnt++;
                reader.close();
            }
            stats.generated = cnt;
        }

        if (fe) {
            Path fetchDir = new Path(segment, CrawlDatum.FETCH_DIR_NAME);
            if (fs.exists(fetchDir) && fs.getFileStatus(fetchDir).isDirectory()) {
                cnt = 0L;
                long start = Long.MAX_VALUE;
                long end = Long.MIN_VALUE;
                CrawlDatum value = new CrawlDatum();
                MapFile.Reader[] mreaders = MapFileOutputFormat.getReaders(fetchDir, getConf());
                for (MapFile.Reader mreader : mreaders) {
                    while (mreader.next(key, value)) {
                        cnt++;
                        if (value.getFetchTime() < start)
                            start = value.getFetchTime();
                        if (value.getFetchTime() > end)
                            end = value.getFetchTime();
                    }
                    mreader.close();
                }
                stats.start = start;
                stats.end = end;
                stats.fetched = cnt;
            }
        }

        if (pd) {
            Path parseDir = new Path(segment, ParseData.DIR_NAME);
            if (fs.exists(parseDir) && fs.getFileStatus(parseDir).isDirectory()) {
                cnt = 0L;
                long errors = 0L;
                ParseData value = new ParseData();
                MapFile.Reader[] mreaders = MapFileOutputFormat.getReaders(parseDir, getConf());
                for (MapFile.Reader mreader : mreaders) {
                    while (mreader.next(key, value)) {
                        cnt++;
                        if (!value.getStatus().isSuccess())
                            errors++;
                    }
                    mreader.close();
                }
                stats.parsed = cnt;
                stats.parseErrors = errors;
            }
        }
    }

    private static final int MODE_DUMP = 0;

    private static final int MODE_LIST = 1;

    private static final int MODE_GET = 2;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            usage();
            return;
        }
        int mode = -1;
        switch (args[0]) {
            case "-dump":
                mode = MODE_DUMP;
                break;
            case "-list":
                mode = MODE_LIST;
                break;
            case "-get":
                mode = MODE_GET;
                break;
        }

        boolean co = true;
        boolean fe = true;
        boolean ge = true;
        boolean pa = true;
        boolean pd = true;
        boolean pt = true;
        // collect general options
        for (int i = 1; i < args.length; i++) {
            switch (args[i]) {
                case "-nocontent":
                    co = false;
                    args[i] = null;
                    break;
                case "-nofetch":
                    fe = false;
                    args[i] = null;
                    break;
                case "-nogenerate":
                    ge = false;
                    args[i] = null;
                    break;
                case "-noparse":
                    pa = false;
                    args[i] = null;
                    break;
                case "-noparsedata":
                    pd = false;
                    args[i] = null;
                    break;
                case "-noparsetext":
                    pt = false;
                    args[i] = null;
                    break;
            }
        }
        Configuration conf = NutchConfiguration.create();
        final FileSystem fs = FileSystem.get(conf);
        SegmentReader segmentReader = new SegmentReader(conf, co, fe, ge, pa, pd,
                pt);
        // collect required args
        switch (mode) {
            case MODE_DUMP:
                String input = args[1];
                if (input == null) {
                    System.err.println("Missing required argument: <segment_dir>");
                    usage();
                    return;
                }
                String output = args.length > 2 ? args[2] : null;
                if (output == null) {
                    System.err.println("Missing required argument: <output>");
                    usage();
                    return;
                }
                segmentReader.dump(new Path(input), new Path(output));
                return;
            case MODE_LIST:
                ArrayList<Path> dirs = new ArrayList<>();
                for (int i = 1; i < args.length; i++) {
                    if (args[i] == null)
                        continue;
                    if (args[i].equals("-dir")) {
                        Path dir = new Path(args[++i]);
                        FileStatus[] fstats = fs.listStatus(dir,
                                HadoopFSUtil.getPassDirectoriesFilter(fs));
                        Path[] files = HadoopFSUtil.getPaths(fstats);
                        if (files != null && files.length > 0) {
                            dirs.addAll(Arrays.asList(files));
                        }
                    } else
                        dirs.add(new Path(args[i]));
                }
                segmentReader.list(dirs, new OutputStreamWriter(System.out, "UTF-8"));
                return;
            case MODE_GET:
                input = args[1];
                if (input == null) {
                    System.err.println("Missing required argument: <segment_dir>");
                    usage();
                    return;
                }
                String key = args.length > 2 ? args[2] : null;
                if (key == null) {
                    System.err.println("Missing required argument: <keyValue>");
                    usage();
                    return;
                }
                segmentReader.get(new Path(input), new Text(key), new OutputStreamWriter(
                        System.out, "UTF-8"), new HashMap<>());
                return;
            default:
                System.err.println("Invalid operation: " + args[0]);
                usage();
        }
    }

    private static void usage() {
        System.err
                .println("Usage: SegmentReader (-dump ... | -list ... | -get ...) [general options]\n");
        System.err.println("* General options:");
        System.err.println("\t-nocontent\tignore content directory");
        System.err.println("\t-nofetch\tignore crawl_fetch directory");
        System.err.println("\t-nogenerate\tignore crawl_generate directory");
        System.err.println("\t-noparse\tignore crawl_parse directory");
        System.err.println("\t-noparsedata\tignore parse_data directory");
        System.err.println("\t-noparsetext\tignore parse_text directory");
        System.err.println();
        System.err
                .println("* SegmentReader -dump <segment_dir> <output> [general options]");
        System.err
                .println("  Dumps content of a <segment_dir> as a text file to <output>.\n");
        System.err.println("\t<segment_dir>\tname of the segment directory.");
        System.err
                .println("\t<output>\tname of the (non-existent) output directory.");
        System.err.println();
        System.err
                .println("* SegmentReader -list (<segment_dir1> ... | -dir <segments>) [general options]");
        System.err
                .println("  List a synopsis of segments in specified directories, or all segments in");
        System.err
                .println("  a directory <segments>, and print it on System.out\n");
        System.err
                .println("\t<segment_dir1> ...\tlist of segment directories to process");
        System.err
                .println("\t-dir <segments>\t\tdirectory that contains multiple segments");
        System.err.println();
        System.err
                .println("* SegmentReader -get <segment_dir> <keyValue> [general options]");
        System.err
                .println("  Get a specified record from a segment, and print it on System.out.\n");
        System.err.println("\t<segment_dir>\tname of the segment directory.");
        System.err.println("\t<keyValue>\tvalue of the key (url).");
        System.err
                .println("\t\tNote: put double-quotes around strings with spaces.");
    }
}
