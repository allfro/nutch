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

package org.apache.nutch.tools;

//JDK imports

import org.apache.commons.cli.*;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.io.Content;
import org.apache.nutch.util.DumpFileUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.tika.Tika;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

//Commons imports
//Hadoop
//Tika imports

/**
 * <p>
 * The file dumper tool enables one to reverse generate the raw content from
 * Nutch segment data directories.
 * </p>
 * <p>
 * The tool has a number of immediate uses:
 * <ol>
 * <li>one can see what a page looked like at the time it was crawled</li>
 * <li>one can see different media types acquired as part of the crawl</li>
 * <li>it enables us to see webpages before we augment them with additional
 * metadata, this can be handy for providing a provenance trail for your crawl
 * data.</li>
 * </ol>
 * </p>
 * <p>
 * Upon successful completion the tool displays a very convenient JSON snippet
 * detailing the mimetype classifications and the counts of documents which fall
 * into those classifications. An example is as follows:
 * </p>
 *
 * <pre>
 * {@code
 * INFO: File Types:
 *   TOTAL Stats:
 *    [
 *     {"mimeType":"application/xml","count":"19"}
 *     {"mimeType":"image/png","count":"47"}
 *     {"mimeType":"image/jpeg","count":"141"}
 *     {"mimeType":"image/vnd.microsoft.icon","count":"4"}
 *     {"mimeType":"text/plain","count":"89"}
 *     {"mimeType":"video/quicktime","count":"2"}
 *     {"mimeType":"image/gif","count":"63"}
 *     {"mimeType":"application/xhtml+xml","count":"1670"}
 *     {"mimeType":"application/octet-stream","count":"40"}
 *     {"mimeType":"text/html","count":"1863"}
 *   ]
 *
 *   FILTER Stats: 
 *   [
 *     {"mimeType":"image/png","count":"47"}
 *     {"mimeType":"image/jpeg","count":"141"}
 *     {"mimeType":"image/vnd.microsoft.icon","count":"4"}
 *     {"mimeType":"video/quicktime","count":"2"}
 *     {"mimeType":"image/gif","count":"63"}
 *   ]
 * }
 * </pre>
 * <p>
 * In the case above, the tool would have been run with the <b>-mimeType
 * image/png image/jpeg image/vnd.microsoft.icon video/quicktime image/gif</b>
 * flag and corresponding values activated.
 *
 */
public class FileDumper extends Configured {

    private static final Logger LOG = LoggerFactory.getLogger(FileDumper.class);

    private static boolean local;

    public static FileSystem getFileSystem(Configuration conf) throws IOException {
        return (local)?FileSystem.getLocal(conf):FileSystem.get(conf);
    }

    /**
     * Dumps the reverse engineered raw content from the provided segment
     * directories if a parent directory contains more than one segment, otherwise
     * a single segment can be passed as an argument.
     *
     * @param outputDir
     *          the directory you wish to dump the raw content to. This directory
     *          will be created.
     * @param segmentRootDir
     *          a directory containing one or more segments.
     * @param mimeTypes
     *          an array of mime types we have to dump, all others will be
     *          filtered out.
     * @param mimeTypeStats
     * 	      a flag indicating whether mimetype stats should be displayed
     * 	      instead of dumping files.
     * @throws Exception
     */
    public void dump(Path outputDir, Path segmentRootDir, String[] mimeTypes, boolean mimeTypeStats)
            throws Exception {
        if (mimeTypes == null)
            LOG.info("Accepting all mimetypes.");
        // total file counts
        Map<String, Integer> typeCounts = new HashMap<>();
        // filtered file counts
        Map<String, Integer> filteredCounts = new HashMap<>();
        Configuration configuration = getConf();
        FileSystem fs = getFileSystem(configuration);

        RemoteIterator<LocatedFileStatus> segmentDirs = fs.listFiles(segmentRootDir, true);

        if (!segmentDirs.hasNext()) {
            System.err.println(String.format("No segment directories found in [%s]", segmentRootDir));
            return;
        }

        while (segmentDirs.hasNext()) {
            Path segmentPath = segmentDirs.next().getPath();
            if (!segmentPath.toString().matches(".*?/content/part-.*?/data"))
                continue;
            LOG.info("Processing segment: [{}]", segmentPath);
            DataOutputStream doutputStream = null;
            try {
                SequenceFile.Reader reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(segmentPath));

                Writable key = (Writable) reader.getKeyClass().newInstance();
                Content content;

                while (reader.next(key)) {
                    content = new Content();
                    reader.getCurrentValue(content);
                    String url = key.toString();
                    String baseName = FilenameUtils.getBaseName(url);
                    String extension = FilenameUtils.getExtension(url);
                    if (extension == null || (extension != null && extension.equals(""))) {
                        extension = "html";
                    }

                    ByteArrayInputStream bas = null;
                    Boolean filter = false;
                    try {
                        bas = new ByteArrayInputStream(content.getContent());
                        String mimeType = new Tika().detect(content.getContent());
                        collectStats(typeCounts, mimeType);
                        if (mimeType != null) {
                            if (mimeTypes == null
                                    || Arrays.asList(mimeTypes).contains(mimeType)) {
                                collectStats(filteredCounts, mimeType);
                                filter = true;
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        LOG.warn("Tika is unable to detect type for: [{}]", url);
                    } finally {
                        if (bas != null) {
                            try {
                                bas.close();
                            } catch (Exception ignore) {
                            }
                        }
                    }

                    if (filter) {
                        if (!mimeTypeStats) {
                            String urlMD5 = DumpFileUtil.getUrlMD5(url);
                            Path fullDir = DumpFileUtil.createTwoLevelsDirectory(fs, outputDir, urlMD5, true);

                            if (fullDir != null) {
                                Path outputFile = new Path(fullDir, DumpFileUtil.createFileName(urlMD5, baseName, extension));

                                if (!fs.exists(outputFile)) {
                                    LOG.info("Writing: [{}]", outputFile);
                                    FSDataOutputStream output = fs.create(outputFile);
                                    IOUtils.write(content.getContent(), output.getWrappedStream());
                                } else {
                                    LOG.info("Skipping writing: [{}]: file already exists", outputFile);
                                }
                            }
                        }
                    }
                }
                reader.close();
            } catch(Exception ignored) {
                LOG.info("Skipping content located at {} due to parsing error", segmentPath);
            } finally {
                if (doutputStream != null) {
                    try {
                        doutputStream.close();
                    } catch (Exception ignore) {
                    }
                }
            }
        }
        fs.close();
        LOG.info("Dumper File Stats: {}",
                DumpFileUtil.displayFileTypes(typeCounts, filteredCounts));

        if (mimeTypeStats) {
            System.out.println(String.format("Dumper File Stats: %s",
                    DumpFileUtil.displayFileTypes(typeCounts, filteredCounts)));
        }
    }

    /**
     * Main method for invoking this tool
     *
     * @param args
     *          1) output directory (which will be created) to host the raw data
     *          and 2) a directory containing one or more segments.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // boolean options
        Option helpOpt = new Option("h", "help", false, "show this help message");
        // argument options
        @SuppressWarnings("static-access")
        Option outputOpt = OptionBuilder
                .withArgName("outputDir")
                .hasArg()
                .withDescription(
                        "output directory (which will be created) to host the raw data")
                .create("outputDir");
        @SuppressWarnings("static-access")
        Option segOpt = OptionBuilder.withArgName("segment").hasArgs()
                .withDescription("the segment(s) to use").create("segment");
        @SuppressWarnings("static-access")
        Option mimeOpt = OptionBuilder
                .withArgName("mimetype")
                .hasArgs()
                .withDescription(
                        "an optional list of mimetypes to dump, excluding all others. Defaults to all.")
                .create("mimetype");
        @SuppressWarnings("static-access")
        Option mimeStatOpt = OptionBuilder
                .withArgName("mimeStats")
                .withDescription(
                        "only display mimetype stats for the segment(s) instead of dumping file.")
                .create("mimeStats");

        @SuppressWarnings("static-access")
        Option runLocalOpt = OptionBuilder
                .withArgName("runLocal")
                .withDescription("run this against the local file system segments")
                .create("runLocal");

        // create the options
        Options options = new Options();
        options.addOption(helpOpt);
        options.addOption(outputOpt);
        options.addOption(segOpt);
        options.addOption(mimeOpt);
        options.addOption(mimeStatOpt);
        options.addOption(runLocalOpt);

        CommandLineParser parser = new GnuParser();
        try {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption("help") || !line.hasOption("outputDir")
                    || (!line.hasOption("segment"))) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("FileDumper", options, true);
                return;
            }
            local = line.hasOption("runLocal");

            Configuration conf = NutchConfiguration.create();

            FileSystem fs = getFileSystem(conf);
            Path outputDir = new Path(line.getOptionValue("outputDir"));
            Path segmentRootDir = new Path(line.getOptionValue("segment"));
            String[] mimeTypes = line.getOptionValues("mimetype");
            boolean shouldDisplayStats = false;
            if (line.hasOption("mimeStats"))
                shouldDisplayStats = true;

            if (!fs.exists(outputDir)) {
                LOG.warn("Output directory: [{}]: does not exist, creating it.", outputDir);
                if (!shouldDisplayStats) {
                    if (!fs.mkdirs(outputDir))
                        throw new Exception("Unable to create: [" + outputDir + "]");
                }
            }

            FileDumper dumper = new FileDumper();
            dumper.setConf(conf);
            dumper.dump(outputDir, segmentRootDir, mimeTypes, shouldDisplayStats);
        } catch (Exception e) {
            LOG.error("FileDumper: {}", StringUtils.stringifyException(e));
            e.printStackTrace();
        }
    }

    private void collectStats(Map<String, Integer> typeCounts, String mimeType) {
        typeCounts.put(mimeType,
                typeCounts.containsKey(mimeType) ? typeCounts.get(mimeType) + 1 : 1);
    }

}
