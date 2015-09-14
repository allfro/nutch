package org.apache.nutch.lib.output;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.PrintStream;

/** Implements a text output format */
public class TextOutputFormat extends
        FileOutputFormat<WritableComparable<?>, Writable> {

    @Override
    public RecordWriter<WritableComparable<?>, Writable> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {

        FileSystem fs = FileSystem.get(context.getConfiguration());
        final Path segmentDumpFile = new Path(
                FileOutputFormat.getOutputPath(context), getUniqueFile(context, getOutputName(context), ""));

        // Get the old copy out of the way
        if (fs.exists(segmentDumpFile))
            fs.delete(segmentDumpFile, true);

        final PrintStream printStream = new PrintStream(
                fs.create(segmentDumpFile));

        return new RecordWriter<WritableComparable<?>, Writable>() {
            @Override
            public synchronized void write(WritableComparable<?> key, Writable value)
                    throws IOException {
                printStream.println(value);
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                printStream.close();
            }
        };
    }
}
