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

package org.apache.nutch.indexer;

// Commons Logging imports

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.io.Inlinks;
import org.apache.nutch.io.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.plugin.PluginRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates and caches {@link IndexingFilter} implementing plugins. */
public class IndexingFilters {

  public static final String INDEXINGFILTER_ORDER = "indexingfilter.order";

  public final static Logger LOG = LoggerFactory
      .getLogger(IndexingFilters.class);

  private IndexingFilter[] indexingFilters;

  public IndexingFilters(Configuration conf) {
    indexingFilters = (IndexingFilter[]) PluginRepository.get(conf)
        .getOrderedPlugins(IndexingFilter.class, IndexingFilter.X_POINT_ID,
            INDEXINGFILTER_ORDER);
  }

  /** Run all defined filters. */
  public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
      CrawlDatum datum, Inlinks inlinks) throws IndexingException {
      for (IndexingFilter indexingFilter : this.indexingFilters) {
          doc = indexingFilter.filter(doc, parse, url, datum, inlinks);
          // break the loop if an indexing filter discards the doc
          if (doc == null)
              return null;
      }

    return doc;
  }

}