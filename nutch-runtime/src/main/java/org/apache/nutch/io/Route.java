package org.apache.nutch.io;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A link path or route looking to identify a link cycle.
 */
public class Route implements Writable {

  private String outlinkUrl = null;
  private String lookingFor = null;
  private boolean found = false;

  public Route() {

  }

  public String getOutlinkUrl() {
    return outlinkUrl;
  }

  public void setOutlinkUrl(String outlinkUrl) {
    this.outlinkUrl = outlinkUrl;
  }

  public String getLookingFor() {
    return lookingFor;
  }

  public void setLookingFor(String lookingFor) {
    this.lookingFor = lookingFor;
  }

  public boolean isFound() {
    return found;
  }

  public void setFound(boolean found) {
    this.found = found;
  }

  public void readFields(DataInput in) throws IOException {

    outlinkUrl = Text.readString(in);
    lookingFor = Text.readString(in);
    found = in.readBoolean();
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, outlinkUrl);
    Text.writeString(out, lookingFor);
    out.writeBoolean(found);
  }
}
