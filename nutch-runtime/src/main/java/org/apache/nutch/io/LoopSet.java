package org.apache.nutch.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * A set of loops.
 */
public class LoopSet implements Writable {

  private Set<String> loopSet = new HashSet<String>();

  public LoopSet() {

  }

  public Set<String> getLoopSet() {
    return loopSet;
  }

  public void setLoopSet(Set<String> loopSet) {
    this.loopSet = loopSet;
  }

  public void readFields(DataInput in) throws IOException {

    int numNodes = in.readInt();
    loopSet = new HashSet<String>();
    for (int i = 0; i < numNodes; i++) {
      String url = Text.readString(in);
      loopSet.add(url);
    }
  }

  public void write(DataOutput out) throws IOException {

    int numNodes = (loopSet != null ? loopSet.size() : 0);
    out.writeInt(numNodes);
    for (String loop : loopSet) {
      Text.writeString(out, loop);
    }
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    for (String loop : loopSet) {
      builder.append(loop + ",");
    }
    return builder.substring(0, builder.length() - 1);
  }
}
