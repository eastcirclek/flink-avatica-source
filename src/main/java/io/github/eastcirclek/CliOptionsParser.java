/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.eastcirclek;

import org.apache.commons.cli.*;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.client.SqlClientException;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CliOptionsParser {
  public static final Option OPTION_ENVIRONMENT = Option
    .builder("e")
    .required(false)
    .longOpt("environment")
    .numberOfArgs(1)
    .argName("environment file")
    .build();

  public static final Option OPTION_JAR = Option
    .builder("j")
    .required(false)
    .longOpt("jar")
    .numberOfArgs(1)
    .argName("JAR file")
    .build();

  private static final Options EMBEDDED_MODE_CLIENT_OPTIONS = getEmbeddedModeClientOptions(new Options());

  public static Options getEmbeddedModeClientOptions(Options options) {
    options.addOption(OPTION_ENVIRONMENT);
    options.addOption(OPTION_JAR);
    return options;
  }

  public static CliOptions parseEmbeddedModeClient(String[] args) {
    try {
      DefaultParser parser = new DefaultParser();
      CommandLine line = parser.parse(EMBEDDED_MODE_CLIENT_OPTIONS, args, true);
      return new CliOptions(
        checkUrl(line, CliOptionsParser.OPTION_ENVIRONMENT),
        checkUrls(line, CliOptionsParser.OPTION_JAR)
      );
    }
    catch (ParseException e) {
      throw new SqlClientException(e.getMessage());
    }
  }

  private static URL checkUrl(CommandLine line, Option option) {
    final List<URL> urls = checkUrls(line, option);
    if (urls != null && !urls.isEmpty()) {
      return urls.get(0);
    }
    return null;
  }

  private static List<URL> checkUrls(CommandLine line, Option option) {
    if (line.hasOption(option.getOpt())) {
      final String[] urls = line.getOptionValues(option.getOpt());
      return Arrays.stream(urls)
        .distinct()
        .map((url) -> {
          try {
            return Path.fromLocalFile(new File(url).getAbsoluteFile()).toUri().toURL();
          } catch (Exception e) {
            throw new SqlClientException("Invalid path for option '" + option.getLongOpt() + "': " + url, e);
          }
        })
        .collect(Collectors.toList());
    }
    return null;
  }
}
