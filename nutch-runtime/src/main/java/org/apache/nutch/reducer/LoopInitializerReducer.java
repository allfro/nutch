package org.apache.nutch.reducer;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.nutch.io.LinkDatum;
import org.apache.nutch.io.Node;
import org.apache.nutch.io.Route;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ndouba on 9/2/15.
 */
public class LoopInitializerReducer extends Reducer<Text, ObjectWritable, Text, Route> {

    @Override
    /**
     * Takes any node that has inlinks and sets up a route for all of its
     * outlinks. These routes will then be followed to a maximum depth inside of
     * the Looper job.
     */
    protected void reduce(Text key, Iterable<ObjectWritable> values, Context context)
            throws IOException, InterruptedException {
        String url = key.toString();
        Node node = null;
        List<LinkDatum> outlinkList = new ArrayList<LinkDatum>();

        // collect all outlinks and assign node
        for (ObjectWritable objWrite: values) {
            Object obj = objWrite.get();
            if (obj instanceof LinkDatum) {
                outlinkList.add((LinkDatum) obj);
            } else if (obj instanceof Node) {
                node = (Node) obj;
            }
        }

        // has to have inlinks otherwise cycle not possible
        if (node != null) {

            int numInlinks = node.getNumInlinks();
            if (numInlinks > 0) {

                // initialize and collect a route for every outlink
                for (LinkDatum datum : outlinkList) {
                    String outlinkUrl = datum.getUrl();
                    Route route = new Route();
                    route.setFound(false);
                    route.setLookingFor(url);
                    route.setOutlinkUrl(outlinkUrl);
                    context.write(new Text(outlinkUrl), route);
                }
            }
        }
    }
}
