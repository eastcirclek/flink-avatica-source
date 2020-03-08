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

import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

import java.util.Map;

import static io.github.eastcirclek.AvaticaJDBCValidator.CONNECTOR_TYPE_VALUE_JDBC_AVATICA;
import static org.apache.flink.table.descriptors.JDBCValidator.*;

public class AvaticaJDBC extends ConnectorDescriptor {
  private String url;
  private String driver;
  private String username;
  private String password;
  private String table;

  public AvaticaJDBC() {
    super(CONNECTOR_TYPE_VALUE_JDBC_AVATICA, 1, false);
  }

  public AvaticaJDBC url(String url) {
    Preconditions.checkNotNull(url);
    this.url = url;
    return this;
  }

  public AvaticaJDBC driver(String driver) {
    Preconditions.checkNotNull(driver);
    this.driver = driver;
    return this;
  }

  public AvaticaJDBC username(String username) {
    Preconditions.checkNotNull(username);
    this.username = username;
    return this;
  }

  public AvaticaJDBC password(String password) {
    Preconditions.checkNotNull(password);
    this.password = password;
    return this;
  }

  public AvaticaJDBC table(String table) {
    Preconditions.checkNotNull(table);
    this.table = table;
    return this;
  }

  @Override
  protected Map<String, String> toConnectorProperties() {
    final DescriptorProperties properties = new DescriptorProperties();

    if (url != null) {
      properties.putString(CONNECTOR_URL, url);
    }

    if (driver != null) {
      properties.putString(CONNECTOR_DRIVER, driver);
    }

    if (username != null) {
      properties.putString(CONNECTOR_USERNAME, username);
    }

    if (password != null) {
      properties.putString(CONNECTOR_PASSWORD, password);
    }

    if (table != null) {
      properties.putString(CONNECTOR_TABLE, table);
    }

    return properties.asMap();
  }
}
