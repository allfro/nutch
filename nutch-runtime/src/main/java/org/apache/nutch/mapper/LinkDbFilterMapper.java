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
import org.apache.nutch.io.Inlink;
import org.apache.nutch.io.Inlinks;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * This class provides a way to separate the URL normalization and filtering
 * steps from the rest of LinkDb manipulation code.
 *
 * @author Andrzej Bialecki
 */
public class LinkDbFilterMapper extends Mapper<Text, Inlinks, Text, Inlinks> {
    public static final String URL_FILTERING = "linkdb.url.filters";

    public static final String URL_NORMALIZING = "linkdb.url.normalizer";

    public static final String URL_NORMALIZING_SCOPE = "linkdb.url.normalizer.scope";

    private boolean filter;

    private boolean normalize;

    private URLFilters filters;

    private URLNormalizers normalizers;

    private String scope;

    public static final Logger LOG = LoggerFactory.getLogger(LinkDbFilterMapper.class);

    private Text newKey = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        filter = configuration.getBoolean(URL_FILTERING, false);
        normalize = configuration.getBoolean(URL_NORMALIZING, false);
        if (filter) {
            filters = new URLFilters(configuration);
        }
        if (normalize) {
            scope = configuration.get(URL_NORMALIZING_SCOPE, URLNormalizers.SCOPE_LINKDB);
            normalizers = new URLNormalizers(configuration, scope);
        }
    }

    @Override
    protected void map(Text key, Inlinks value, Context context) throws IOException, InterruptedException {
        String url = key.toString();
        Inlinks result = new Inlinks();
        if (normalize) {
            try {
                url = normalizers.normalize(url, scope); // normalize the url
            } catch (Exception e) {
                LOG.warn("Skipping {}: {}", url, e);
                url = null;
            }
        }
        if (url != null && filter) {
            try {
                url = filters.filter(url); // filter the url
            } catch (Exception e) {
                LOG.warn("Skipping {}: {}", url, e);
                url = null;
            }
        }
        if (url == null)
            return; // didn't pass the filters
        Iterator<Inlink> it = value.iterator();
        String fromUrl = null;
        while (it.hasNext()) {
            Inlink inlink = it.next();
            fromUrl = inlink.getFromUrl();
            if (normalize) {
                try {
                    fromUrl = normalizers.normalize(fromUrl, scope); // normalize the url
                } catch (Exception e) {
                    LOG.warn("Skipping {}: {}", fromUrl, e);
                    fromUrl = null;
                }
            }
            if (fromUrl != null && filter) {
                try {
                    fromUrl = filters.filter(fromUrl); // filter the url
                } catch (Exception e) {
                    LOG.warn("Skipping {}: {}", fromUrl, e);
                    fromUrl = null;
                }
            }
            if (fromUrl != null) {
                result.add(new Inlink(fromUrl, inlink.getAnchor()));
            }
        }
        if (result.size() > 0) { // don't collect empty inlinks
            newKey.set(url);
            context.write(newKey, result);
        }
    }
}
