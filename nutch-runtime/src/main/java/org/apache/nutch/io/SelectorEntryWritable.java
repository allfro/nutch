package org.apache.nutch.io;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SelectorEntryWritable implements Writable {
  public Text url;
  public CrawlDatum datum;
  public IntWritable segnum;

  public SelectorEntryWritable() {
    url = new Text();
    datum = new CrawlDatum();
    segnum = new IntWritable(0);
  }

  public void readFields(DataInput in) throws IOException {
    url.readFields(in);
    datum.readFields(in);
    segnum.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    url.write(out);
    datum.write(out);
    segnum.write(out);
  }

  public String toString() {
    return "url=" + url.toString() + ", datum=" + datum.toString()
            + ", segnum=" + segnum.toString();
  }
}
