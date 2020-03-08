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

import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.github.eastcirclek.AvaticaJDBCValidator.CONNECTOR_TYPE_VALUE_JDBC_AVATICA;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.*;
import static org.apache.flink.table.descriptors.JDBCValidator.*;
import static org.apache.flink.table.descriptors.Schema.*;

/**
 * Factory for creating configured instances of {@link JDBCTableSource}
 */
public class AvaticaJDBCSourceFactory implements StreamTableSourceFactory<Row> {

  public AvaticaJDBCSourceFactory() {

  }

  @Override
  public Map<String, String> requiredContext() {
    Map<String, String> context = new HashMap<>();
    context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_JDBC_AVATICA); // jdbc-avatica
    context.put(CONNECTOR_PROPERTY_VERSION, "1"); // backwards compatibility
    return context;
  }

  @Override
  public List<String> supportedProperties() {
    List<String> properties = new ArrayList<>();

    // common options
    properties.add(CONNECTOR_DRIVER);
    properties.add(CONNECTOR_URL);
    properties.add(CONNECTOR_TABLE);
    properties.add(CONNECTOR_USERNAME);
    properties.add(CONNECTOR_PASSWORD);

    // schema
    properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
    properties.add(SCHEMA + ".#." + SCHEMA_NAME);
    // computed column
    properties.add(SCHEMA + ".#." + TABLE_SCHEMA_EXPR);

    // watermark
    properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_ROWTIME);
    properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_EXPR);
    properties.add(SCHEMA + "." + WATERMARK + ".#."  + WATERMARK_STRATEGY_DATA_TYPE);

    return properties;
  }

  @Override
  public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
    DescriptorProperties descriptorProperties = getValidatedProperties(properties);
    TableSchema schema = TableSchemaUtils.getPhysicalSchema(
      descriptorProperties.getTableSchema(SCHEMA));

    return JDBCTableSource.builder()
      .setOptions(getJDBCOptions(descriptorProperties))
      .setSchema(schema)
      .build();
  }

  private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
    final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
    descriptorProperties.putProperties(properties);

    new SchemaValidator(true, false, false).validate(descriptorProperties);
    new AvaticaJDBCValidator().validate(descriptorProperties); // use AvaticaJDBCValidator instead of JDBCValidator

    return descriptorProperties;
  }

  private JDBCOptions getJDBCOptions(DescriptorProperties descriptorProperties) {
    final String url = descriptorProperties.getString(CONNECTOR_URL);
    final JDBCOptions.Builder builder = JDBCOptions.builder()
      .setDBUrl(url)
      .setTableName(descriptorProperties.getString(CONNECTOR_TABLE))
      .setDialect(new AvaticaJDBCDialect());

    descriptorProperties.getOptionalString(CONNECTOR_DRIVER).ifPresent(builder::setDriverName);
    descriptorProperties.getOptionalString(CONNECTOR_USERNAME).ifPresent(builder::setUsername);
    descriptorProperties.getOptionalString(CONNECTOR_PASSWORD).ifPresent(builder::setPassword);

    return builder.build();
  }
}
