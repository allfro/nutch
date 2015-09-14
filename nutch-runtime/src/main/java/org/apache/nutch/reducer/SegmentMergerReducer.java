package org.apache.nutch.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.io.Content;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.io.ParseText;
import org.apache.nutch.metadata.MetaWrapper;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.segment.SegmentMergeFilters;
import org.apache.nutch.segment.SegmentMerger;
import org.apache.nutch.segment.SegmentPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Created by ndouba on 9/2/15.
 */
public class SegmentMergerReducer extends Reducer<Text, MetaWrapper, Text, MetaWrapper> {


    private static final Logger LOG = LoggerFactory.getLogger(SegmentMergerReducer.class);
    private SegmentMergeFilters mergeFilters = null;
    private long sliceSize = -1;
    private long curCount = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        if (configuration.getBoolean("segment.merger.filter", false)) {
            mergeFilters = new SegmentMergeFilters(configuration);
        }

        sliceSize = configuration.getLong("segment.merger.slice", -1);

        if ((sliceSize > 0) && (LOG.isInfoEnabled())) LOG.info("Slice size: {} URLs.", sliceSize);
        if (sliceSize > 0) {
            sliceSize = sliceSize / context.getNumReduceTasks();
        }
    }

    @Override
    /**
     * NOTE: in selecting the latest version we rely exclusively on the segment
     * name (not all segment data contain time information). Therefore it is
     * extremely important that segments be named in an increasing lexicographic
     * order as their creation time increases.
     */
    public void reduce(Text key, Iterable<MetaWrapper> values, Context context)
            throws IOException, InterruptedException {
        CrawlDatum lastG = null;
        CrawlDatum lastF = null;
        CrawlDatum lastSig = null;
        Content lastC = null;
        ParseData lastPD = null;
        ParseText lastPT = null;
        String lastGname = null;
        String lastFname = null;
        String lastSigname = null;
        String lastCname = null;
        String lastPDname = null;
        String lastPTname = null;
        TreeMap<String, ArrayList<CrawlDatum>> linked = new TreeMap<>();
        for (MetaWrapper wrapper: values) {
            Object o = wrapper.get();
            String spString = wrapper.getMeta(SegmentMerger.SEGMENT_PART_KEY);
            if (spString == null) {
                throw new IOException("Null segment part, key=" + key);
            }
            SegmentPart sp = SegmentPart.parse(spString);
            if (o instanceof CrawlDatum) {
                CrawlDatum val = (CrawlDatum) o;
                // check which output dir it belongs to
                switch (sp.partName) {
                    case CrawlDatum.GENERATE_DIR_NAME:
                        if (lastG == null) {
                            lastG = val;
                            lastGname = sp.segmentName;
                        } else {
                            // take newer
                            if (lastGname.compareTo(sp.segmentName) < 0) {
                                lastG = val;
                                lastGname = sp.segmentName;
                            }
                        }
                        break;
                    case CrawlDatum.FETCH_DIR_NAME:
                        // only consider fetch status and ignore fetch retry status
                        // https://issues.apache.org/jira/browse/NUTCH-1520
                        // https://issues.apache.org/jira/browse/NUTCH-1113
                        if (CrawlDatum.hasFetchStatus(val)
                                && val.getStatus() != CrawlDatum.STATUS_FETCH_RETRY
                                && val.getStatus() != CrawlDatum.STATUS_FETCH_NOTMODIFIED) {
                            if (lastF == null) {
                                lastF = val;
                                lastFname = sp.segmentName;
                            } else {
                                if (lastFname.compareTo(sp.segmentName) < 0) {
                                    lastF = val;
                                    lastFname = sp.segmentName;
                                }
                            }
                        }
                        break;
                    case CrawlDatum.PARSE_DIR_NAME:
                        if (val.getStatus() == CrawlDatum.STATUS_SIGNATURE) {
                            if (lastSig == null) {
                                lastSig = val;
                                lastSigname = sp.segmentName;
                            } else {
                                // take newer
                                if (lastSigname.compareTo(sp.segmentName) < 0) {
                                    lastSig = val;
                                    lastSigname = sp.segmentName;
                                }
                            }
                            continue;
                        }
                        // collect all LINKED values from the latest segment
                        ArrayList<CrawlDatum> segLinked = linked.get(sp.segmentName);
                        if (segLinked == null) {
                            segLinked = new ArrayList<CrawlDatum>();
                            linked.put(sp.segmentName, segLinked);
                        }
                        segLinked.add(val);
                        break;
                    default:
                        throw new IOException("Cannot determine segment part: " + sp.partName);
                }
            } else if (o instanceof Content) {
                if (lastC == null) {
                    lastC = (Content) o;
                    lastCname = sp.segmentName;
                } else {
                    if (lastCname.compareTo(sp.segmentName) < 0) {
                        lastC = (Content) o;
                        lastCname = sp.segmentName;
                    }
                }
            } else if (o instanceof ParseData) {
                if (lastPD == null) {
                    lastPD = (ParseData) o;
                    lastPDname = sp.segmentName;
                } else {
                    if (lastPDname.compareTo(sp.segmentName) < 0) {
                        lastPD = (ParseData) o;
                        lastPDname = sp.segmentName;
                    }
                }
            } else if (o instanceof ParseText) {
                if (lastPT == null) {
                    lastPT = (ParseText) o;
                    lastPTname = sp.segmentName;
                } else {
                    if (lastPTname.compareTo(sp.segmentName) < 0) {
                        lastPT = (ParseText) o;
                        lastPTname = sp.segmentName;
                    }
                }
            }
        }
        // perform filtering based on full merge record
        if (mergeFilters != null
                && !mergeFilters.filter(key, lastG, lastF, lastSig, lastC, lastPD,
                lastPT, linked.isEmpty() ? null : linked.lastEntry().getValue())) {
            return;
        }

        curCount++;
        String sliceName = null;
        MetaWrapper wrapper = new MetaWrapper();
        if (sliceSize > 0) {
            sliceName = String.valueOf(curCount / sliceSize);
            wrapper.setMeta(SegmentMerger.SEGMENT_SLICE_KEY, sliceName);
        }
        SegmentPart sp = new SegmentPart();
        // now output the latest values
        if (lastG != null) {
            wrapper.set(lastG);
            sp.partName = CrawlDatum.GENERATE_DIR_NAME;
            sp.segmentName = lastGname;
            wrapper.setMeta(SegmentMerger.SEGMENT_PART_KEY, sp.toString());
            context.write(key, wrapper);
        }
        if (lastF != null) {
            wrapper.set(lastF);
            sp.partName = CrawlDatum.FETCH_DIR_NAME;
            sp.segmentName = lastFname;
            wrapper.setMeta(SegmentMerger.SEGMENT_PART_KEY, sp.toString());
            context.write(key, wrapper);
        }
        if (lastSig != null) {
            wrapper.set(lastSig);
            sp.partName = CrawlDatum.PARSE_DIR_NAME;
            sp.segmentName = lastSigname;
            wrapper.setMeta(SegmentMerger.SEGMENT_PART_KEY, sp.toString());
            context.write(key, wrapper);
        }
        if (lastC != null) {
            wrapper.set(lastC);
            sp.partName = Content.DIR_NAME;
            sp.segmentName = lastCname;
            wrapper.setMeta(SegmentMerger.SEGMENT_PART_KEY, sp.toString());
            context.write(key, wrapper);
        }
        if (lastPD != null) {
            wrapper.set(lastPD);
            sp.partName = ParseData.DIR_NAME;
            sp.segmentName = lastPDname;
            wrapper.setMeta(SegmentMerger.SEGMENT_PART_KEY, sp.toString());
            context.write(key, wrapper);
        }
        if (lastPT != null) {
            wrapper.set(lastPT);
            sp.partName = ParseText.DIR_NAME;
            sp.segmentName = lastPTname;
            wrapper.setMeta(SegmentMerger.SEGMENT_PART_KEY, sp.toString());
            context.write(key, wrapper);
        }
        if (linked.size() > 0) {
            String name = linked.lastKey();
            sp.partName = CrawlDatum.PARSE_DIR_NAME;
            sp.segmentName = name;
            wrapper.setMeta(SegmentMerger.SEGMENT_PART_KEY, sp.toString());
            ArrayList<CrawlDatum> segLinked = linked.get(name);
            for (CrawlDatum link : segLinked) {
                wrapper.set(link);
                context.write(key, wrapper);
            }
        }
    }
}
