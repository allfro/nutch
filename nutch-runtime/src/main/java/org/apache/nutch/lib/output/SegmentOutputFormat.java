package org.apache.nutch.lib.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.nutch.io.Content;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.io.ParseText;
import org.apache.nutch.metadata.MetaWrapper;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.segment.SegmentMerger;
import org.apache.nutch.segment.SegmentPart;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;

/**
 * Created by ndouba on 9/2/15.
 */
public class SegmentOutputFormat extends
        FileOutputFormat<Text, MetaWrapper> {
    private static final String DEFAULT_SLICE = "default";

    @Override
    public RecordWriter<Text, MetaWrapper> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {

        Configuration configuration = context.getConfiguration();
        FileSystem fs = FileSystem.get(configuration);

        return new RecordWriter<Text, MetaWrapper>() {
            MapFile.Writer c_out = null;
            MapFile.Writer f_out = null;
            MapFile.Writer pd_out = null;
            MapFile.Writer pt_out = null;
            SequenceFile.Writer g_out = null;
            SequenceFile.Writer p_out = null;
            HashMap<String, Closeable> sliceWriters = new HashMap<>();
            String segmentName = configuration.get("segment.merger.segmentName");

            public void write(Text key, MetaWrapper wrapper) throws IOException {
                // unwrap
                SegmentPart sp = SegmentPart.parse(wrapper.getMeta(SegmentMerger.SEGMENT_PART_KEY));
                Writable o = wrapper.get();
                String slice = wrapper.getMeta(SegmentMerger.SEGMENT_SLICE_KEY);
                if (o instanceof CrawlDatum) {
                    switch (sp.partName) {
                        case CrawlDatum.GENERATE_DIR_NAME:
                            g_out = ensureSequenceFile(slice, CrawlDatum.GENERATE_DIR_NAME);
                            g_out.append(key, o);
                            break;
                        case CrawlDatum.FETCH_DIR_NAME:
                            f_out = ensureMapFile(slice, CrawlDatum.FETCH_DIR_NAME,
                                    CrawlDatum.class);
                            f_out.append(key, o);
                            break;
                        case CrawlDatum.PARSE_DIR_NAME:
                            p_out = ensureSequenceFile(slice, CrawlDatum.PARSE_DIR_NAME);
                            p_out.append(key, o);
                            break;
                        default:
                            throw new IOException("Cannot determine segment part: "
                                    + sp.partName);
                    }
                } else if (o instanceof Content) {
                    c_out = ensureMapFile(slice, Content.DIR_NAME, Content.class);
                    c_out.append(key, o);
                } else if (o instanceof ParseData) {
                    // update the segment name inside contentMeta - required by Indexer
                    if (slice == null) {
                        ((ParseData) o).getContentMeta().set(Nutch.SEGMENT_NAME_KEY,
                                segmentName);
                    } else {
                        ((ParseData) o).getContentMeta().set(Nutch.SEGMENT_NAME_KEY,
                                segmentName + "-" + slice);
                    }
                    pd_out = ensureMapFile(slice, ParseData.DIR_NAME, ParseData.class);
                    pd_out.append(key, o);
                } else if (o instanceof ParseText) {
                    pt_out = ensureMapFile(slice, ParseText.DIR_NAME, ParseText.class);
                    pt_out.append(key, o);
                }
            }

            @Override
            public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                for (Closeable o : sliceWriters.values()) {
                    o.close();
                }
            }

            // lazily create SequenceFile-s.
            private SequenceFile.Writer ensureSequenceFile(String slice,
                                                           String dirName) throws IOException {
                if (slice == null)
                    slice = DEFAULT_SLICE;
                SequenceFile.Writer res = (SequenceFile.Writer) sliceWriters
                        .get(slice + dirName);
                if (res != null)
                    return res;
                Path wname;
                Path out = FileOutputFormat.getOutputPath(context);
                if (Objects.equals(slice, DEFAULT_SLICE)) {
                    wname = new Path(new Path(new Path(out, segmentName), dirName),
                            getUniqueFile(context, getOutputName(context), ""));
                } else {
                    wname = new Path(new Path(new Path(out, segmentName + "-" + slice),
                            dirName), getUniqueFile(context, getOutputName(context), ""));
                }

//          Option rKeyClassOpt = MapFile.Writer.keyClass(Text.class);
//          org.apache.hadoop.io.SequenceFile.Writer.Option rValClassOpt = SequenceFile.Writer.valueClass(CrawlDatum.class);
//          Option rProgressOpt = (Option) SequenceFile.Writer.progressable(progress);
//          Option rCompOpt = (Option) SequenceFile.Writer.compression(SequenceFileOutputFormat.getOutputCompressionType(job));
//          Option rFileOpt = (Option) SequenceFile.Writer.file(wname);

                //res = SequenceFile.createWriter(job, rFileOpt, rKeyClassOpt,
                //   rValClassOpt, rCompOpt, rProgressOpt);

                res = SequenceFile.createWriter(configuration, SequenceFile.Writer.file(wname),
                        SequenceFile.Writer.keyClass(Text.class),
                        SequenceFile.Writer.valueClass(CrawlDatum.class),
                        SequenceFile.Writer.bufferSize(fs.getConf().getInt("io.file.buffer.size", 4096)),
                        SequenceFile.Writer.replication(fs.getDefaultReplication(wname)),
                        SequenceFile.Writer.blockSize(1073741824),
                        SequenceFile.Writer.compression(SequenceFileOutputFormat.getOutputCompressionType(context), new DefaultCodec()),
                        SequenceFile.Writer.progressable(context),
                        SequenceFile.Writer.metadata(new SequenceFile.Metadata()));

                sliceWriters.put(slice + dirName, res);
                return res;
            }

            // lazily create MapFile-s.
            private MapFile.Writer ensureMapFile(String slice, String dirName,
                                                 Class<? extends Writable> clazz) throws IOException {
                if (slice == null)
                    slice = DEFAULT_SLICE;
                MapFile.Writer res = (MapFile.Writer) sliceWriters.get(slice
                        + dirName);
                if (res != null)
                    return res;
                Path wname;
                Path out = FileOutputFormat.getOutputPath(context);
                if (Objects.equals(slice, DEFAULT_SLICE)) {
                    wname = new Path(new Path(new Path(out, segmentName), dirName),
                            getUniqueFile(context, getOutputName(context), ""));
                } else {
                    wname = new Path(new Path(new Path(out, segmentName + "-" + slice),
                            dirName),  getUniqueFile(context, getOutputName(context), ""));
                }
                SequenceFile.CompressionType compType = SequenceFileOutputFormat
                        .getOutputCompressionType(context);
                if (clazz.isAssignableFrom(ParseText.class)) {
                    compType = SequenceFile.CompressionType.RECORD;
                }

                MapFile.Writer.Option rKeyClassOpt = MapFile.Writer.keyClass(Text.class);
                SequenceFile.Writer.Option rValClassOpt = SequenceFile.Writer.valueClass(clazz);
                SequenceFile.Writer.Option rProgressOpt = SequenceFile.Writer.progressable(context);
                SequenceFile.Writer.Option rCompOpt = SequenceFile.Writer.compression(compType);

                res = new MapFile.Writer(configuration, wname, rKeyClassOpt,
                        rValClassOpt, rCompOpt, rProgressOpt);
                sliceWriters.put(slice + dirName, res);
                return res;
            }

        };
    }
}
