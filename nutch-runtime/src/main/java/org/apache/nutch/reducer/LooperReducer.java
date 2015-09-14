package org.apache.nutch.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.io.Route;

import java.io.IOException;
import java.util.*;

/**
 * Created by ndouba on 9/2/15.
 */
public class LooperReducer extends Reducer<Text, ObjectWritable, Text, Route> {

    private Configuration conf;
    private boolean last = false;

    /**
     * Configure the job.
     */
    public void setup(Context context) {
        this.conf = context.getConfiguration();
        this.last = conf.getBoolean("last", false);
    }


    @Override
    /**
     * Performs a single loop pass looking for loop cycles within routes. If
     * This is not the last loop cycle then url will be mapped for further
     * passes.
     */
    protected void reduce(Text key, Iterable<ObjectWritable> values, Context context)
            throws IOException, InterruptedException {

        List<Route> routeList = new ArrayList<>();
        Set<String> outlinkUrls = new LinkedHashSet<>();
        int numValues = 0;

        // aggregate all routes and outlinks for a given url
        for (ObjectWritable next: values) {
            Object value = next.get();
            if (value instanceof Route) {
                routeList.add(WritableUtils.clone((Route) value, conf));
            } else if (value instanceof Text) {
                String outlinkUrl = value.toString();
                if (!outlinkUrls.contains(outlinkUrl)) {
                    outlinkUrls.add(outlinkUrl);
                }
            }

            // specify progress, could be a lot of routes
            numValues++;
            if (numValues % 100 == 0) {
                context.progress();
            }
        }

        // loop through the route list
        Iterator<Route> routeIt = routeList.listIterator();
        while (routeIt.hasNext()) {

            // removing the route for space concerns, could be a lot of routes
            // if the route is already found, meaning it is a loop just collect it
            // urls with no outlinks that are not found will fall off
            Route route = routeIt.next();
            routeIt.remove();
            if (route.isFound()) {
                context.write(key, route);
            } else {

                // if the route start url is found, set route to found and collect
                String lookingFor = route.getLookingFor();
                if (outlinkUrls.contains(lookingFor)) {
                    route.setFound(true);
                    context.write(key, route);
                } else if (!last) {

                    // setup for next pass through the loop
                    for (String outlink : outlinkUrls) {
                        context.write(new Text(outlink), route);
                    }
                }
            }
        }
    }

}
