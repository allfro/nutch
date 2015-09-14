package org.apache.nutch.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Bean class which holds url to node information.
 */
public class LinkNode implements Writable {

  private String url = null;
  private Node node = null;

  public LinkNode() {

  }

  public LinkNode(String url, Node node) {
    this.url = url;
    this.node = node;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public Node getNode() {
    return node;
  }

  public void setNode(Node node) {
    this.node = node;
  }

  public void readFields(DataInput in) throws IOException {
    url = in.readUTF();
    node = new Node();
    node.readFields(in);
  }

  public void write(DataOutput out) throws IOException {
    out.writeUTF(url);
    node.write(out);
  }

}
