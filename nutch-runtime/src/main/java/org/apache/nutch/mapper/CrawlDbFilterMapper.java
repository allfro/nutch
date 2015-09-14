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

package org.apache.nutch.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.io.CrawlDatum;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class provides a way to separate the URL normalization and filtering
 * steps from the rest of CrawlDb manipulation code.
 *
 * @author Andrzej Bialecki
 */
public class CrawlDbFilterMapper extends
        Mapper<Text, CrawlDatum, Text, CrawlDatum> {
    public static final String URL_FILTERING = "crawldb.url.filters";

    public static final String URL_NORMALIZING = "crawldb.url.normalizers";

    public static final String URL_NORMALIZING_SCOPE = "crawldb.url.normalizers.scope";

    private boolean urlFiltering;

    private boolean urlNormalizers;

    private boolean url404Purging;

    private URLFilters filters;

    private URLNormalizers normalizers;

    private String scope;

    public static final Logger LOG = LoggerFactory.getLogger(CrawlDbFilterMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        urlFiltering = configuration.getBoolean(URL_FILTERING, false);
        urlNormalizers = configuration.getBoolean(URL_NORMALIZING, false);
        url404Purging = configuration.getBoolean(CrawlDb.CRAWLDB_PURGE_404, false);

        if (urlFiltering) {
            filters = new URLFilters(configuration);
        }
        if (urlNormalizers) {
            scope = configuration.get(URL_NORMALIZING_SCOPE, URLNormalizers.SCOPE_CRAWLDB);
            normalizers = new URLNormalizers(configuration, scope);
        }
    }

    @Override
    protected void map(Text key, CrawlDatum value, Context context)
            throws IOException, InterruptedException {

        Text newKey = new Text();
        String url = key.toString();

        // https://issues.apache.org/jira/browse/NUTCH-1101 check status first,
        // cheaper than normalizing or filtering
        if (url404Purging && CrawlDatum.STATUS_DB_GONE == value.getStatus()) {
            url = null;
        }
        if (url != null && urlNormalizers) {
            try {
                url = normalizers.normalize(url, scope); // normalize the url
            } catch (Exception e) {
                LOG.warn("Skipping {}: {}", url, e);
                url = null;
            }
        }
        if (url != null && urlFiltering) {
            try {
                url = filters.filter(url); // filter the url
            } catch (Exception e) {
                LOG.warn("Skipping {}: {}", url, e);
                url = null;
            }
        }
        if (url != null) { // if it passes
            newKey.set(url); // collect it
            context.write(newKey, value);
        }
    }
}
