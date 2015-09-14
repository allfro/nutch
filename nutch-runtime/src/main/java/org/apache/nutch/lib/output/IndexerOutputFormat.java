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
package org.apache.nutch.lib.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.nutch.indexer.IndexWriters;
import org.apache.nutch.io.NutchIndexAction;

import java.io.IOException;

public class IndexerOutputFormat extends
        FileOutputFormat<Text, NutchIndexAction> {

    @Override
    public RecordWriter<Text, NutchIndexAction> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {
        Configuration configuration = job.getConfiguration();
        final IndexWriters writers = new IndexWriters(configuration);

        writers.open(configuration, getUniqueFile(job, getOutputName(job), ""));

        return new RecordWriter<Text, NutchIndexAction>() {

            @Override
            public void write(Text key, NutchIndexAction indexAction)
                    throws IOException {
                if (indexAction.action == NutchIndexAction.ADD) {
                    writers.write(indexAction.doc);
                } else if (indexAction.action == NutchIndexAction.DELETE) {
                    writers.delete(key.toString());
                }
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                writers.close();
            }
        };
    }
}
