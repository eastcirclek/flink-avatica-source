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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;

import java.sql.*;

public class FlinkSQLUtils {
  /**
   * A utility method to convert a JDBC type to a corresponding Flink type
   * @param jdbcType a string representing JDBC types
   * @return a Flink's internal type
   */
  static DataType jdbcType2FlinkDataType(String jdbcType) {
    String type = jdbcType;
    if (jdbcType.equals("VARCHAR")) {
      type = "STRING";
    } else if (jdbcType.equals("TIMESTAMP")) {
      type = "TIMESTAMP(3)";
    }
    LogicalType lt = LogicalTypeParser.parse(type);
    DataType dt = LogicalTypeDataTypeConverter.toDataType(lt);

    return dt;
  }

  static TableSchema getDruidTableSchema(String url, String username, String password, String table) throws SQLException {
    TableSchema.Builder schemaBuilder = TableSchema.builder();
    try (Connection connection = DriverManager.getConnection(url, username, password)) {
      DatabaseMetaData metaData = connection.getMetaData();
      ResultSet columns = metaData.getColumns(null, null, table, null);
      while (columns.next()) { // .next() moves forward the cursor of the result set
        String columnName = columns.getString("COLUMN_NAME");
        JDBCType jdbcType = JDBCType.valueOf(columns.getInt("DATA_TYPE"));
        DataType flinkType = FlinkSQLUtils.jdbcType2FlinkDataType(jdbcType.getName());
        schemaBuilder.field(columnName, flinkType);
      }
    }
    schemaBuilder.watermark("__time", "__time", DataTypes.TIMESTAMP());
    TableSchema schema = schemaBuilder.build();
    
    return schema;
  }
}
