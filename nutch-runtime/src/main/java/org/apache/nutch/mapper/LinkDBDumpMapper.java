package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.Inlinks;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ndouba on 9/2/15.
 */
public class LinkDBDumpMapper extends Mapper<Text, Inlinks, Text, Inlinks> {
    Pattern pattern = null;
    Matcher matcher = null;

    @Override
    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
        if (configuration.get("linkdb.regex", null) != null) {
            pattern = Pattern.compile(configuration.get("linkdb.regex"));
        }
    }

    @Override
    public void map(Text key, Inlinks value, Context context)
            throws IOException, InterruptedException {

        if (pattern != null) {
            matcher = pattern.matcher(key.toString());
            if (!matcher.matches()) {
                return;
            }
        }

        context.write(key, value);
    }
}
