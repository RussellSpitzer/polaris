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
package org.apache.polaris.service.catalog;

import static org.apache.polaris.service.context.DefaultContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.ExternalCatalog;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.PolarisCatalog;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.PolarisApplication;
import org.apache.polaris.service.config.PolarisApplicationConfig;
import org.apache.polaris.service.test.PolarisConnectionExtension;
import org.apache.polaris.service.test.PolarisRealm;
import org.apache.polaris.service.test.TestEnvironmentExtension;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

@ExtendWith({
  DropwizardExtensionsSupport.class,
  TestEnvironmentExtension.class,
  PolarisConnectionExtension.class
})
public class PolarisSparkForeignTableIntegrationTest {
  private static final DropwizardAppExtension<PolarisApplicationConfig> EXT =
      new DropwizardAppExtension<>(
          PolarisApplication.class,
          ResourceHelpers.resourceFilePath("polaris-server-integrationtest.yml"),
          ConfigOverride.config(
              "server.applicationConnectors[0].port",
              "0"), // Bind to random port to support parallelism
          ConfigOverride.config(
              "server.adminConnectors[0].port", "0")); // Bind to random port to support parallelism

  public static final String CATALOG_NAME = "mycatalog";
  public static final String EXTERNAL_CATALOG_NAME = "external_catalog";
  private static PolarisConnectionExtension.PolarisToken polarisToken;
  private static SparkSession spark;
  private String realm;
  private static Path testDir;

  @BeforeAll
  public static void setup(
      PolarisConnectionExtension.PolarisToken polarisToken, @PolarisRealm String realm)
      throws IOException {
    PolarisSparkForeignTableIntegrationTest.polarisToken = polarisToken;

    // Set up test location
    testDir = Path.of("build/test_data/iceberg/" + realm);
    FileUtils.deleteQuietly(testDir.toFile());
    Files.createDirectories(testDir);
  }

  @AfterAll
  public static void cleanup() {}

  @BeforeEach
  public void before(@PolarisRealm String realm) {
    this.realm = realm;
    FileStorageConfigInfo storageConfigInfo =
        FileStorageConfigInfo.builder(StorageConfigInfo.StorageTypeEnum.FILE)
            .setAllowedLocations(List.of("file://" + testDir.toFile().getAbsolutePath()))
            .build();

    CatalogProperties props = new CatalogProperties("file://" + testDir.toFile().getAbsolutePath());
    Catalog catalog =
        PolarisCatalog.builder()
            .setType(Catalog.TypeEnum.INTERNAL)
            .setName(CATALOG_NAME)
            .setProperties(props)
            .setStorageConfigInfo(storageConfigInfo)
            .build();

    try (Response response =
        EXT.client()
            .target(
                String.format("http://localhost:%d/api/management/v1/catalogs", EXT.getLocalPort()))
            .request("application/json")
            .header("Authorization", "BEARER " + polarisToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(catalog))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    CatalogProperties externalProps =
        new CatalogProperties("file://" + testDir.toFile().getAbsolutePath());
    Catalog externalCatalog =
        ExternalCatalog.builder()
            .setType(Catalog.TypeEnum.EXTERNAL)
            .setName(EXTERNAL_CATALOG_NAME)
            .setProperties(externalProps)
            .setStorageConfigInfo(storageConfigInfo)
            .setRemoteUrl("http://dummy_url")
            .build();
    try (Response response =
        EXT.client()
            .target(
                String.format("http://localhost:%d/api/management/v1/catalogs", EXT.getLocalPort()))
            .request("application/json")
            .header("Authorization", "BEARER " + polarisToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(externalCatalog))) {
      assertThat(response).returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
    SparkSession.Builder sessionBuilder =
        SparkSession.builder()
            .master("local[1]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.ui.showConsoleProgress", false)
            .config("spark.ui.enabled", "false");
    spark =
        withCatalog(withCatalog(sessionBuilder, CATALOG_NAME), EXTERNAL_CATALOG_NAME).getOrCreate();

    onSpark("USE " + CATALOG_NAME);
  }

  private SparkSession.Builder withCatalog(SparkSession.Builder builder, String catalogName) {
    return builder
        .config(
            String.format("spark.sql.catalog.%s", catalogName),
            "org.apache.iceberg.spark.SparkCatalog")
        .config(String.format("spark.sql.catalog.%s.type", catalogName), "rest")
        .config(
            String.format("spark.sql.catalog.%s.uri", catalogName),
            "http://localhost:" + EXT.getLocalPort() + "/api/catalog")
        .config(String.format("spark.sql.catalog.%s.warehouse", catalogName), catalogName)
        .config(String.format("spark.sql.catalog.%s.scope", catalogName), "PRINCIPAL_ROLE:ALL")
        .config(String.format("spark.sql.catalog.%s.header.realm", catalogName), realm)
        .config(String.format("spark.sql.catalog.%s.token", catalogName), polarisToken.token())
        .config(String.format("spark.sql.catalog.%s.s3.access-key-id", catalogName), "fakekey")
        .config(
            String.format("spark.sql.catalog.%s.s3.secret-access-key", catalogName), "fakesecret")
        .config(String.format("spark.sql.catalog.%s.s3.region", catalogName), "us-west-2");
  }

  @AfterEach
  public void after() {
    cleanupCatalog(CATALOG_NAME);
    cleanupCatalog(EXTERNAL_CATALOG_NAME);
    try {
      SparkSession.clearDefaultSession();
      SparkSession.clearActiveSession();
      spark.close();
    } catch (Exception e) {
      LoggerFactory.getLogger(getClass()).error("Unable to close spark session", e);
    }
  }

  private void cleanupCatalog(String catalogName) {
    onSpark("USE " + catalogName);
    List<Row> namespaces = onSpark("SHOW NAMESPACES").collectAsList();
    for (Row namespace : namespaces) {
      List<Row> tables = onSpark("SHOW TABLES IN " + namespace.getString(0)).collectAsList();
      for (Row table : tables) {
        onSpark("DROP TABLE " + namespace.getString(0) + "." + table.getString(1));
      }
      List<Row> views = onSpark("SHOW VIEWS IN " + namespace.getString(0)).collectAsList();
      for (Row view : views) {
        onSpark("DROP VIEW " + namespace.getString(0) + "." + view.getString(1));
      }
      onSpark("DROP NAMESPACE " + namespace.getString(0));
    }
    try (Response response =
        EXT.client()
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/catalogs/" + catalogName,
                    EXT.getLocalPort()))
            .request("application/json")
            .header("Authorization", "BEARER " + polarisToken.token())
            .header(REALM_PROPERTY_KEY, realm)
            .delete()) {
      assertThat(response).returns(Response.Status.NO_CONTENT.getStatusCode(), Response::getStatus);
    }
  }

  @Test
  public void testCreateForeignTable() throws IOException {
    File targetDir = new File(testDir.toFile(), "ns1");
    DeltaTable deltaTable = new DeltaTable(targetDir.getAbsolutePath());
    deltaTable.create();

    long namespaceCount = onSpark("SHOW NAMESPACES").count();
    assertThat(namespaceCount).isEqualTo(0L);

    onSpark("CREATE NAMESPACE ns1");
    onSpark("USE ns1");

    onSpark(
        "CREATE TABLE tbl1 (col1 integer, col2 string) using iceberg TBLPROPERTIES('_source'='delta') location '"
            + targetDir.getAbsolutePath()
            + "'");
    assertThat(onSpark("SELECT * FROM tbl1").count()).isEqualTo(6);
    deltaTable.append();
    onSpark("REFRESH TABLE tbl1");
    assertThat(onSpark("SELECT * FROM tbl1").count()).isEqualTo(12);

    FileUtils.deleteDirectory(targetDir);
  }

  private static Dataset<Row> onSpark(@Language("SQL") String sql) {
    return spark.sql(sql);
  }

  private static class DeltaTable {
    private String tableLocation;
    private SparkSession sparkSession;
    private StructType schema;

    DeltaTable(String location) {
      tableLocation = Path.of(location).toString();

      SparkSession.Builder sessionBuilder =
          SparkSession.builder()
              .master("local[1]")
              .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
              .config(
                  "spark.sql.catalog.spark_catalog",
                  "org.apache.spark.sql.delta.catalog.DeltaCatalog");
      sparkSession = sessionBuilder.getOrCreate();
    }

    void create() {
      schema =
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
      var dataFrame = sparkSession.createDataFrame(rows, schema);
      dataFrame
          .write()
          .format("delta")
          .partitionBy("city")
          .mode(SaveMode.Overwrite)
          .save(tableLocation);
    }

    void append() {
      var rows =
          List.of(
              RowFactory.create(1, "John", 25, "NYC", "2023-09-28 00:00:00"),
              RowFactory.create(2, "Emily", 30, "SFO", "2023-09-28 00:00:00"),
              RowFactory.create(3, "Michael", 35, "ORD", "2023-09-28 00:00:00"),
              RowFactory.create(4, "Andrew", 40, "NYC", "2023-10-28 00:00:00"),
              RowFactory.create(5, "Bob", 28, "SEA", "2023-09-23 00:00:00"),
              RowFactory.create(6, "Charlie", 31, "DFW", "2023-08-29 00:00:00"));
      var dataFrame = sparkSession.createDataFrame(rows, schema);
      dataFrame
          .write()
          .format("delta")
          .partitionBy("city")
          .mode(SaveMode.Append)
          .save(tableLocation);
    }
  }
}
