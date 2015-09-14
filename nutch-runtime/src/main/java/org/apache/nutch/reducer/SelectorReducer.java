package org.apache.nutch.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.io.SelectorEntryWritable;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;

public class SelectorReducer extends
        Reducer<FloatWritable, SelectorEntryWritable, FloatWritable, SelectorEntryWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(SelectorReducer.class);
    private LongWritable genTime = new LongWritable(System.currentTimeMillis());
    private long limit;
    private long count;
    private HashMap<String, int[]> hostCounts = new HashMap<>();
    private int segCounts[];
    private int maxCount;
    private boolean byDomain = false;
    private URLNormalizers normalizers;
    private boolean normalise;
    private int maxNumSegments = 1;
    int currentsegmentnum = 1;
    private Counter malformedURLs;
    MultipleOutputs<FloatWritable, SelectorEntryWritable> out;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        limit = configuration.getLong(Generator.GENERATOR_TOP_N, Long.MAX_VALUE)
                / context.getNumReduceTasks();
        maxCount = configuration.getInt(Generator.GENERATOR_MAX_COUNT, -1);
        if (maxCount == -1) {
            byDomain = false;
        }
        if (Generator.GENERATOR_COUNT_VALUE_DOMAIN.equals(configuration.get(Generator.GENERATOR_COUNT_MODE)))
            byDomain = true;
        normalise = configuration.getBoolean(Generator.GENERATOR_NORMALISE, true);
        if (normalise)
            normalizers = new URLNormalizers(configuration,
                    URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
        long time = configuration.getLong(Nutch.GENERATE_TIME_KEY, 0L);
        if (time > 0)
            genTime.set(time);
        maxNumSegments = configuration.getInt(Generator.GENERATOR_MAX_NUM_SEGMENTS, 1);
        segCounts = new int[maxNumSegments];
        malformedURLs = context.getCounter("Generator", "MALFORMED_URL");
        out = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(FloatWritable key, Iterable<SelectorEntryWritable> values, Context context) throws IOException, InterruptedException {

        for (SelectorEntryWritable entry : values) {

            if (count == limit) {
                // do we have any segments left?
                if (currentsegmentnum < maxNumSegments) {
                    count = 0;
                    currentsegmentnum++;
                } else
                    break;
            }

            Text urlText = entry.url;
            String urlString = urlText.toString();
            URL url;

            String hostordomain;

            try {
                if (normalise && normalizers != null) {
                    urlString = normalizers.normalize(urlString,
                            URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
                }
                url = new URL(urlString);
                if (byDomain) {
                    hostordomain = URLUtil.getDomainName(url);
                } else {
                    hostordomain = new URL(urlString).getHost();
                }
            } catch (Exception e) {
                LOG.warn("Malformed URL: '{}', skipping ({})",
                        urlString,
                        StringUtils.stringifyException(e)
                );
                malformedURLs.increment(1);
                continue;
            }

            hostordomain = hostordomain.toLowerCase();

            // only filter if we are counting hosts or domains
            if (maxCount > 0) {
                int[] hostCount = hostCounts.get(hostordomain);
                if (hostCount == null) {
                    hostCount = new int[] { 1, 0 };
                    hostCounts.put(hostordomain, hostCount);
                }

                // increment hostCount
                hostCount[1]++;

                // check if topN reached, select next segment if it is
                while (segCounts[hostCount[0] - 1] >= limit
                        && hostCount[0] < maxNumSegments) {
                    hostCount[0]++;
                    hostCount[1] = 0;
                }

                // reached the limit of allowed URLs per host / domain
                // see if we can put it in the next segment?
                if (hostCount[1] >= maxCount) {
                    if (hostCount[0] < maxNumSegments) {
                        hostCount[0]++;
                        hostCount[1] = 0;
                    } else {
                        if (hostCount[1] == maxCount + 1 && LOG.isInfoEnabled()) {
                            LOG.info("Host or domain {} has more than {} URLs for all {}" +
                                            " segments. Additional URLs won't be included in the fetchlist.",
                                    hostordomain,
                                    maxCount,
                                    maxNumSegments
                            );
                        }
                        // skip this entry
                        continue;
                    }
                }
                entry.segnum = new IntWritable(hostCount[0]);
                segCounts[hostCount[0] - 1]++;
            } else {
                entry.segnum = new IntWritable(currentsegmentnum);
                segCounts[currentsegmentnum - 1]++;
            }

            out.write("out", key, entry, generateFileName(entry));

            // Count is incremented only when we keep the URL
            // maxCount may cause us to skip it.
            count++;
        }
    }

    protected String generateFileName(SelectorEntryWritable value) {
        return "fetchlist-" + value.segnum.toString() + "/part";
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        out.close();
    }
}
