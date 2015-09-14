package org.apache.nutch.mapper;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.CrawlDatum;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ndouba on 9/2/15.
 */
public class CrawlDbDumpMapper extends
        Mapper<Text, CrawlDatum, Text, CrawlDatum> {
    Pattern pattern = null;
    Matcher matcher = null;
    String status = null;
    Expression expr = null;
    Integer retry = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        if (configuration.get("regex", null) != null) {
            pattern = Pattern.compile(configuration.get("regex"));
        }
        status = configuration.get("status", null);
        retry = configuration.getInt("retry", -1);

        if (configuration.get("expr", null) != null) {
            JexlEngine jexl = new JexlEngine();
            jexl.setSilent(true);
            jexl.setStrict(true);
            expr = jexl.createExpression(configuration.get("expr", null));
        }
    }

    @Override
    protected void map(Text key, CrawlDatum value, Context context) throws IOException, InterruptedException {
        // check retry
        if (retry != -1) {
            if (value.getRetriesSinceFetch() < retry) {
                return;
            }
        }

        // check status
        if (status != null
                && !status.equalsIgnoreCase(CrawlDatum.getStatusName(value
                .getStatus())))
            return;

        // check regex
        if (pattern != null) {
            matcher = pattern.matcher(key.toString());
            if (!matcher.matches()) {
                return;
            }
        }

        // check expr
        if (expr != null) {
            if (!value.evaluate(expr)) {
                return;
            }
        }

        context.write(key, value);
    }
}
