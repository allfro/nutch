package org.apache.nutch.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.io.NutchWritable;

import java.io.IOException;

/**
 * Created by ndouba on 9/2/15.
 */
public class InputCompatMapper extends Mapper<WritableComparable<?>, Writable, Text, NutchWritable> {
  private Text newKey = new Text();

    @Override
    protected void map(WritableComparable<?> key, Writable value, Context context)
            throws IOException, InterruptedException {
    // convert on the fly from old formats with UTF8 keys.
    // UTF8 deprecated and replaced by Text.
    if (key instanceof Text) {
      newKey.set(key.toString());
      key = newKey;
    }
    context.write((Text) key, new NutchWritable(value));
  }

}
