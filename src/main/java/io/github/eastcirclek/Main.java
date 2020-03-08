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

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static io.github.eastcirclek.AvaticaJDBCDialect.AVATICA_JDBC_DRIVER;

public class Main {
  public static void main(String[] args) throws Exception {
    final ParameterTool param = ParameterTool.fromArgs(args);
    final String url = param.get("url");
    final String username = param.get("username", "admin");
    final String password = param.get("password", "admin");
    final String table = param.get("table");

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.setParallelism(1);

    EnvironmentSettings settings = EnvironmentSettings.newInstance()
        .inStreamingMode()
        .useBlinkPlanner()
        .build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    AvaticaJDBC avatica = new AvaticaJDBC()
      .url(url)
      .driver(AVATICA_JDBC_DRIVER)
      .username(username)
      .password(password)
      .table(table);

    Schema schema = new Schema()
      .schema(
        FlinkSQLUtils.getDruidTableSchema(url, username, password, table)
      );

    tableEnv
      .connect(avatica)
      .withSchema(schema)
      .createTemporaryTable("sensor");

    tableEnv.toAppendStream(
      tableEnv.sqlQuery("select * from sensor"), Row.class
    ).print();

    env.execute();
  }
}
