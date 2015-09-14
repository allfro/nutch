package org.apache.nutch.lib.output;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.util.StringUtil;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * Created by ndouba on 9/2/15.
 */
public class CrawlDatumCsvOutputFormat extends
        FileOutputFormat<Text, CrawlDatum> {

    @Override
    public RecordWriter<Text, CrawlDatum> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Path dir = FileOutputFormat.getOutputPath(context);
        String name = getUniqueFile(context, getOutputName(context), "");
        FileSystem fs = FileSystem.get(context.getConfiguration());
        DataOutputStream fileOut = fs.create(new Path(dir, name), context);
        return new LineRecordWriter(fileOut);
    }

    protected static class LineRecordWriter extends RecordWriter<Text, CrawlDatum> {
        private DataOutputStream out;

        public LineRecordWriter(DataOutputStream out) {
            this.out = out;
            try {
                out.writeBytes("Url,Status code,Status name,Fetch Time,Modified Time,Retries since fetch,Retry interval seconds,Retry interval days,Score,Signature,Metadata\n");
            } catch (IOException ignored) {
            }
        }

        @Override
        public synchronized void write(Text key, CrawlDatum value)
                throws IOException {
            out.writeByte('"');
            out.writeBytes(key.toString());
            out.writeByte('"');
            out.writeByte(',');
            out.writeBytes(Integer.toString(value.getStatus()));
            out.writeByte(',');
            out.writeByte('"');
            out.writeBytes(CrawlDatum.getStatusName(value.getStatus()));
            out.writeByte('"');
            out.writeByte(',');
            out.writeBytes(new Date(value.getFetchTime()).toString());
            out.writeByte(',');
            out.writeBytes(new Date(value.getModifiedTime()).toString());
            out.writeByte(',');
            out.writeBytes(Integer.toString(value.getRetriesSinceFetch()));
            out.writeByte(',');
            out.writeBytes(Float.toString(value.getFetchInterval()));
            out.writeByte(',');
            out.writeBytes(Float.toString((value.getFetchInterval() / FetchSchedule.SECONDS_PER_DAY)));
            out.writeByte(',');
            out.writeBytes(Float.toString(value.getScore()));
            out.writeByte(',');
            out.writeByte('"');
            out.writeBytes(value.getSignature() != null ? StringUtil
                    .toHexString(value.getSignature()) : "null");
            out.writeByte('"');
            out.writeByte(',');
            out.writeByte('"');
            if (value.getMetaData() != null) {
                for (Map.Entry<Writable, Writable> e : value.getMetaData().entrySet()) {
                    out.writeBytes(e.getKey().toString());
                    out.writeByte(':');
                    out.writeBytes(e.getValue().toString());
                    out.writeBytes("|||");
                }
            }
            out.writeByte('"');

            out.writeByte('\n');
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            out.close();
        }
    }
}
