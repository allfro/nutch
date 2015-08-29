/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.util;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.nutch.metadata.Nutch;

public abstract class NutchTool extends Configured {

  protected HashMap<String, Object> results = new HashMap<>();
  protected Map<String, Object> status = Collections.synchronizedMap(new HashMap<>());
  protected Job currentJob;
  protected int numJobs;
  protected int currentJobNum;

  /**
   * Runs the tool, using a map of arguments. May return results, or null.
   */
  public abstract Map<String, Object> run(Map<String, String> args, String crawlId)
      throws Exception;

  public NutchTool(Configuration configuration){
    super(configuration);
  }

  public NutchTool(){
    super(null);
  }

  /** Returns relative progress of the tool, a float in range [0,1]. */
  public float getProgress() {
    float progress = 0;
    if (currentJob != null) {
      try {
        progress = (currentJob.mapProgress() + currentJob.reduceProgress()) / 2.0f;
      } catch (IOException | IllegalStateException e) {
        e.printStackTrace();
        progress = 0;
      }
    }
    // take into account multiple jobs
    if (numJobs > 1) {
      progress = (currentJobNum + progress) / (float) numJobs;
    }
    status.put(Nutch.STAT_PROGRESS, progress);
    return progress;
  }

  /** Returns current status of the running tool. */
  public Map<String, Object> getStatus() {
    return status;
  }

  /**
   * Stop the job with the possibility to resume. Subclasses should override
   * this, since by default it calls {@link #killJob()}.
   * 
   * @return true if succeeded, false otherwise
   */
  public boolean stopJob() throws Exception {
    return killJob();
  }

  /**
   * Kill the job immediately. Clients should assume that any results that the
   * job produced so far are in inconsistent state or missing.
   * 
   * @return true if succeeded, false otherwise.
   * @throws Exception
   */
  public boolean killJob() throws Exception {
    if (currentJob != null && !currentJob.isComplete()) {
      try {
        currentJob.killJob();
        return true;
      } catch (Exception e) {
        e.printStackTrace();
        return false;
      }
    }
    return false;
  }
}