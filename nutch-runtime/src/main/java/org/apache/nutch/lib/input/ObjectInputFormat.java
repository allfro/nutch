package org.apache.nutch.lib.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.nutch.metadata.MetaWrapper;
import org.apache.nutch.segment.SegmentMerger;
import org.apache.nutch.segment.SegmentPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Wraps inputs in an {@link MetaWrapper}, to permit merging different types
 * in reduce and use additional metadata.
 */
public class ObjectInputFormat extends
        SequenceFileInputFormat<Text, MetaWrapper> {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectInputFormat.class);

    @Override
    public RecordReader<Text, MetaWrapper> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        Configuration configuration = context.getConfiguration();
        context.setStatus(split.toString());

        // find part name
        SegmentPart segmentPart;
        final String spString;
        final FileSplit fSplit = (FileSplit) split;
        try {
            segmentPart = SegmentPart.get(fSplit);
            spString = segmentPart.toString();
        } catch (IOException e) {
            throw new RuntimeException("Cannot identify segment:", e);
        }

        SequenceFile.Reader reader = new SequenceFile.Reader(configuration, SequenceFile.Reader.file(fSplit.getPath()));

        final Writable w;
        try {
            w = (Writable) reader.getValueClass().newInstance();
        } catch (Exception e) {
            throw new IOException(e.toString());
        } finally {
            try {
                reader.close();
            } catch (Exception e) {
                // ignore
            }
        }
        final SequenceFileRecordReader<Text, Writable> splitReader = new SequenceFileRecordReader<>();
        try {
            splitReader.initialize(split, context);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }

        SequenceFileRecordReader<Text, MetaWrapper> recordReader = new SequenceFileRecordReader<Text, MetaWrapper>() {
            public synchronized boolean next(Text key, MetaWrapper wrapper)
                    throws IOException, InterruptedException {
                LOG.debug("Running OIF.next()");

                boolean res = splitReader.nextKeyValue();
                wrapper.set(splitReader.getCurrentValue());
                wrapper.setMeta(SegmentMerger.SEGMENT_PART_KEY, spString);
                return res;
            }

            @Override
            public synchronized void close() throws IOException {
                splitReader.close();
            }
        };
        try {
            recordReader.initialize(fSplit, context);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
        return recordReader;
    }
}
