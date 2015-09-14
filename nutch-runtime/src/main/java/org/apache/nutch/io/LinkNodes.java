package org.apache.nutch.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Writable class which holds an array of LinkNode objects.
 */
public class LinkNodes implements Writable {

  private LinkNode[] links;

  public LinkNodes() {

  }

  public LinkNodes(LinkNode[] links) {
    this.links = links;
  }

  public LinkNode[] getLinks() {
    return links;
  }

  public void setLinks(LinkNode[] links) {
    this.links = links;
  }

  public void readFields(DataInput in) throws IOException {
    int numLinks = in.readInt();
    if (numLinks > 0) {
      links = new LinkNode[numLinks];
      for (int i = 0; i < numLinks; i++) {
        LinkNode node = new LinkNode();
        node.readFields(in);
        links[i] = node;
      }
    }
  }

  public void write(DataOutput out) throws IOException {
    if (links != null && links.length > 0) {
      int numLinks = links.length;
      out.writeInt(numLinks);
      for (int i = 0; i < numLinks; i++) {
        links[i].write(out);
      }
    }
  }
}
