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

import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

import static org.apache.flink.table.descriptors.JDBCValidator.*;

public class AvaticaJDBCValidator extends ConnectorDescriptorValidator {

  public static final String CONNECTOR_TYPE_VALUE_JDBC_AVATICA = "jdbc-avatica";

  @Override
  public void validate(DescriptorProperties properties) {
    super.validate(properties);
    validateCommonProperties(properties);
  }

  private void validateCommonProperties(DescriptorProperties properties) {
    properties.validateString(CONNECTOR_URL, false, 1);
    properties.validateString(CONNECTOR_TABLE, false, 1);
    properties.validateString(CONNECTOR_DRIVER, true);
    properties.validateString(CONNECTOR_USERNAME, true);
    properties.validateString(CONNECTOR_PASSWORD, true);

    final String url = properties.getString(CONNECTOR_URL);
    final Optional<JDBCDialect> dialect = Optional.of(new AvaticaJDBCDialect());
    Preconditions.checkState(dialect.isPresent(), "Cannot handle such jdbc url: " + url);

    Optional<String> password = properties.getOptionalString(CONNECTOR_PASSWORD);
    if (password.isPresent()) {
      Preconditions.checkArgument(
        properties.getOptionalString(CONNECTOR_USERNAME).isPresent(),
        "Database username must be provided when database password is provided");
    }
  }
}
