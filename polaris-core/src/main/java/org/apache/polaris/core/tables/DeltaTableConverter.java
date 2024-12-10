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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.polaris.core.entity.ForeignTableEntity;
import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.model.sync.SyncResult;

import java.util.Collections;

/** Use XTable library to convert Delta table to Iceberg table */
public class DeltaTableConverter implements ForeignTableConverter {
  private final Configuration conf;

  public DeltaTableConverter() {
    conf = loadHadoopConf();
  }

  @Override
  public Table convert(ForeignTableEntity entity) throws ConversionFailureException {
    // TODO: checking whether the entity is already converted
    if (!TableFormat.DELTA.equals(entity.getSource())) {
      throw new ConversionFailureException("Invalid source format: " + entity.getSource());
    }
    SyncResult syncResult = runSync(entity);
    if (syncResult.getStatus() != SyncResult.SyncStatus.SUCCESS) {
      throw new ConversionFailureException(syncResult.getStatus().getErrorMessage());
    }
    return loadTable(entity);
  }

  @VisibleForTesting
  SyncResult runSync(ForeignTableEntity entity) {
    ConversionConfig conversionConfig = getConversionConfig(entity);

    DeltaConversionSourceProvider conversionSourceProvider = new DeltaConversionSourceProvider();
    conversionSourceProvider.init(conf);

    ConversionController conversionController = new ConversionController(conf);
    return conversionController
        .sync(conversionConfig, conversionSourceProvider)
        .getOrDefault(
            TableFormat.ICEBERG,
            SyncResult.builder().status(SyncResult.SyncStatus.SUCCESS).build());
  }

  private Table loadTable(ForeignTableEntity entity) {
    String baseLocation = entity.getBaseLocation();
    return new HadoopTables(conf).load(baseLocation);
  }

  private ConversionConfig getConversionConfig(ForeignTableEntity entity) {
    String sourceFormat = entity.getSource();
    String tableBasePath = entity.getBaseLocation();
    String tableName = entity.getName();
    SourceTable sourceTable =
        SourceTable.builder()
            .name(tableName)
            .basePath(tableBasePath)
            .formatName(sourceFormat)
            .build();
    TargetTable targetTable =
        TargetTable.builder()
            .name(tableName)
            .basePath(tableBasePath)
            .formatName(TableFormat.ICEBERG)
            .build();
    return ConversionConfig.builder()
        .sourceTable(sourceTable)
        .targetTables(Collections.singletonList(targetTable))
        .syncMode(SyncMode.INCREMENTAL)
        .build();
  }

  private static Configuration loadHadoopConf() {
    Configuration conf = new Configuration();
    conf.set("spark.master", "local[2]");
    return conf;
  }
}
