package org.apache.nutch.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.LinkDatum;

import java.io.IOException;

/**
 * The InlinkDb creates a database of Inlinks. Inlinks are inverted from the
 * OutlinkDb LinkDatum objects and are regenerated each time the WebGraph is
 * updated.
 */
public class InlinkDbMapper extends Mapper<Text, LinkDatum, Text, LinkDatum> {

    private long timestamp;

    @Override
    /**
     * Inverts the Outlink LinkDatum objects into new LinkDatum objects with a
     * new system timestamp, type and to and from url switched.
     */
    public void map(Text key, LinkDatum datum, Context context)
            throws IOException, InterruptedException {

        // get the to and from url and the anchor
        String fromUrl = key.toString();
        String toUrl = datum.getUrl();
        String anchor = datum.getAnchor();

        // flip the from and to url and set the new link type
        LinkDatum inlink = new LinkDatum(fromUrl, anchor, timestamp);
        inlink.setLinkType(LinkDatum.INLINK);
        context.write(new Text(toUrl), inlink);
    }
}
