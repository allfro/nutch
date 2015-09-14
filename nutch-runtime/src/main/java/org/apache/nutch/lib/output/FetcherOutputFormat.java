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

package org.apache.nutch.lib.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Writer.Option;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.io.Content;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.io.NutchWritable;
import org.apache.nutch.parse.Parse;

import java.io.IOException;

/** Splits FetcherOutput entries into multiple map files. */
public class FetcherOutputFormat extends SequenceFileOutputFormat<Text, NutchWritable> {

    @Override
    public RecordWriter<Text, NutchWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Path out = FileOutputFormat.getOutputPath(context);
        Configuration configuration = context.getConfiguration();

        String name = getUniqueFile(context, getOutputName(context), "");
        final Path fetch = new Path(new Path(out, CrawlDatum.FETCH_DIR_NAME), name);
        final Path content = new Path(new Path(out, Content.DIR_NAME), name);

        final CompressionType compType = SequenceFileOutputFormat.getOutputCompressionType(context);

        Option fKeyClassOpt = MapFile.Writer.keyClass(Text.class);
        org.apache.hadoop.io.SequenceFile.Writer.Option fValClassOpt = SequenceFile.Writer.valueClass(CrawlDatum.class);
        org.apache.hadoop.io.SequenceFile.Writer.Option fProgressOpt = SequenceFile.Writer.progressable(context);
        org.apache.hadoop.io.SequenceFile.Writer.Option fCompOpt = SequenceFile.Writer.compression(compType);

        final MapFile.Writer fetchOut = new MapFile.Writer(configuration,
                fetch, fKeyClassOpt, fValClassOpt, fCompOpt, fProgressOpt);

        return new RecordWriter<Text, NutchWritable>() {
            private MapFile.Writer contentOut;
            private RecordWriter<Text, Parse> parseOut;

            {
                if (Fetcher.isStoringContent(configuration)) {
                    Option cKeyClassOpt = MapFile.Writer.keyClass(Text.class);
                    org.apache.hadoop.io.SequenceFile.Writer.Option cValClassOpt = SequenceFile.Writer.valueClass(Content.class);
                    org.apache.hadoop.io.SequenceFile.Writer.Option cProgressOpt = SequenceFile.Writer.progressable(context);
                    org.apache.hadoop.io.SequenceFile.Writer.Option cCompOpt = SequenceFile.Writer.compression(compType);
                    contentOut = new MapFile.Writer(configuration, content,
                            cKeyClassOpt, cValClassOpt, cCompOpt, cProgressOpt);
                }

                if (Fetcher.isParsing(configuration)) {
                    parseOut = new ParseOutputFormat().getRecordWriter(context);
                }
            }

            @Override
            public void write(Text key, NutchWritable value) throws IOException, InterruptedException {
                Writable w = value.get();

                if (w instanceof CrawlDatum)
                    fetchOut.append(key, w);
                else if (w instanceof Content && contentOut != null)
                    contentOut.append(key, w);
                else if (w instanceof Parse && parseOut != null)
                    parseOut.write(key, (Parse) w);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                fetchOut.close();
                if (contentOut != null) {
                    contentOut.close();
                }
                if (parseOut != null) {
                    parseOut.close(context);
                }
            }

        };
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException {
        Path out = FileOutputFormat.getOutputPath(context);
        if (out == null) {
            if (context.getNumReduceTasks() != 0)
                throw new IOException("Output directory not set in Job's configuration.");
        } else {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            if (fs.exists(new Path(out, CrawlDatum.FETCH_DIR_NAME)))
                throw new IOException("Segment already fetched!");
        }
    }
}
