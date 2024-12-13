/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.tables;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.polaris.core.entity.ForeignTableEntity;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncResult;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DeltaTableConverterTest {
  private static DeltaTableConverter converter;
  private static String baseLocation;
  private SparkSession spark;

  @BeforeAll
  public static void setup() {
    converter = new DeltaTableConverter();
    String tableName = "people";
    String localBasePath = "file:///tmp/delta-dataset";
    // TODO: replace local baseLocation with a S3 endpoint
    baseLocation = Path.of(localBasePath, tableName).toString();
  }

  @BeforeEach
  public void before() {
    SparkSession.Builder sessionBuilder =
        SparkSession.builder()
            .master("local[1]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3.aws.credentials.provider",
                "org.apache.hadoop.fs.s3.TemporaryAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3.access.key", "foo")
            .config("spark.hadoop.fs.s3.secret.key", "bar");
    spark = sessionBuilder.getOrCreate();

    var schema =
        new StructType()
            .add(StructField.apply("id", DataTypes.IntegerType, true, Metadata.empty()))
            .add(StructField.apply("name", DataTypes.StringType, true, Metadata.empty()))
            .add(StructField.apply("age", DataTypes.IntegerType, true, Metadata.empty()))
            .add(StructField.apply("city", DataTypes.StringType, true, Metadata.empty()))
            .add(StructField.apply("create_ts", DataTypes.StringType, true, Metadata.empty()));
    var rows =
        List.of(
            RowFactory.create(1, "John", 25, "NYC", "2023-09-28 00:00:00"),
            RowFactory.create(2, "Emily", 30, "SFO", "2023-09-28 00:00:00"),
            RowFactory.create(3, "Michael", 35, "ORD", "2023-09-28 00:00:00"),
            RowFactory.create(4, "Andrew", 40, "NYC", "2023-10-28 00:00:00"),
            RowFactory.create(5, "Bob", 28, "SEA", "2023-09-23 00:00:00"),
            RowFactory.create(6, "Charlie", 31, "DFW", "2023-08-29 00:00:00"));
    var dataFrame = spark.createDataFrame(rows, schema);
    dataFrame
        .write()
        .format("delta")
        .partitionBy("city")
        .mode(SaveMode.Overwrite)
        .save(baseLocation);
  }

  @AfterEach
  public void after() {
    spark.stop();
    FileUtils.deleteQuietly(new File(baseLocation.replace("file:", "")));
  }

  @Test
  void testRunSync() {
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "people");
    final String metadataLocation = "dummy_location";

    ForeignTableEntity entity =
        new ForeignTableEntity.Builder(identifier, metadataLocation)
            .setSource(TableFormat.DELTA)
            .setBaseLocation(baseLocation)
            .setName(identifier.name())
            .build();
    SyncResult syncResult = converter.runSync(entity);
    Assertions.assertThat(syncResult.getStatus()).isEqualTo(SyncResult.SyncStatus.SUCCESS);
  }

  @Test
  void testConvert() {
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "people");
    final String metadataLocation = "dummy_location";

    ForeignTableEntity entity =
        new ForeignTableEntity.Builder(identifier, metadataLocation)
            .setSource(TableFormat.DELTA)
            .setBaseLocation(baseLocation)
            .setName(identifier.name())
            .build();
    Table table = converter.convert(entity);
    Assertions.assertThat(table.schema().columns().stream().map(Types.NestedField::name))
        .containsExactlyInAnyOrder("id", "name", "age", "city", "create_ts");
  }
}
