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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class DumpFileUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DumpFileUtil.class);

    private final static String FILENAME_PATTERN = "%s_%s.%s";
    private final static Integer MAX_LENGTH_OF_FILENAME = 32;
    private final static Integer MAX_LENGTH_OF_EXTENSION = 5;

    public static String getUrlMD5(String url) {
        return MD5Hash.digest(url).toString();
    }

    public static Path createTwoLevelsDirectory(FileSystem fileSystem, Path basePath, String md5, boolean makeDir) {
        Path firstLevelPath = new Path(String.format("%c%c", md5.charAt(0), md5.charAt(8)));
        Path secondLevelPath = new Path(String.format("%c%c", md5.charAt(16), md5.charAt(24)));

        Path fullDirPath = new Path(
                basePath,
                new Path(
                        firstLevelPath,
                        secondLevelPath
                )
        );

        if (makeDir) {
            try {
                fileSystem.mkdirs(fullDirPath);
            } catch (IOException e) {
                LOG.error("Failed to create dir: {}", fullDirPath);
                return null;
            }
        }

        return fullDirPath;
    }

    public static Path createTwoLevelsDirectory(FileSystem fileSystem, Path basePath, String md5) {
        return createTwoLevelsDirectory(fileSystem, basePath, md5, true);
    }

    public static String createFileName(String md5, String fileBaseName, String fileExtension) {
        if (fileBaseName.length() > MAX_LENGTH_OF_FILENAME) {
            LOG.info("File name is too long. Truncated to {} characters.", MAX_LENGTH_OF_FILENAME);
            fileBaseName = StringUtils.substring(fileBaseName, 0, MAX_LENGTH_OF_FILENAME);
        }

        if (fileExtension.length() > MAX_LENGTH_OF_EXTENSION) {
            LOG.info("File extension is too long. Truncated to {} characters.", MAX_LENGTH_OF_EXTENSION);
            fileExtension = StringUtils.substring(fileExtension, 0, MAX_LENGTH_OF_EXTENSION);
        }

        return String.format(FILENAME_PATTERN, md5, fileBaseName, fileExtension);
    }

    public static Path createFileNameFromUrl(FileSystem fs, Path basePath, String reverseKey, String urlString,
                                               String epochScrapeTime, String fileExtension, boolean makeDir) {

        Path fullDirPath = new Path(
                basePath,
                new Path(reverseKey, DigestUtils.shaHex(urlString))
        );

        if (makeDir) {
            try {
               fs.mkdirs(fullDirPath);
            } catch (IOException e) {
                LOG.error("Failed to create dir: {}", fullDirPath);
                return null;
            }
        }

        if (fileExtension.length() > MAX_LENGTH_OF_EXTENSION) {
            LOG.info("File extension is too long. Truncated to {} characters.", MAX_LENGTH_OF_EXTENSION);
            fileExtension = StringUtils.substring(fileExtension, 0, MAX_LENGTH_OF_EXTENSION);
        }

        return new Path(fullDirPath, epochScrapeTime + "." + fileExtension);
    }

    public static String displayFileTypes(Map<String, Integer> typeCounts, Map<String, Integer> filteredCounts) {
        StringBuilder builder = new StringBuilder();
        // print total stats
        builder.append("\nTOTAL Stats:\n");
        builder.append("[\n");
        for (String mimeType : typeCounts.keySet()) {
            builder.append("    {\"mimeType\":\"");
            builder.append(mimeType);
            builder.append("\",\"count\":\"");
            builder.append(typeCounts.get(mimeType));
            builder.append("\"}\n");
        }
        builder.append("]\n");
        // filtered types stats
        if (!filteredCounts.isEmpty()) {
            builder.append("\nFILTERED Stats:\n");
            builder.append("[\n");
            for (String mimeType : filteredCounts.keySet()) {
                builder.append("    {\"mimeType\":\"");
                builder.append(mimeType);
                builder.append("\",\"count\":\"");
                builder.append(filteredCounts.get(mimeType));
                builder.append("\"}\n");
            }
            builder.append("]\n");
        }
        return builder.toString();
    }
}
