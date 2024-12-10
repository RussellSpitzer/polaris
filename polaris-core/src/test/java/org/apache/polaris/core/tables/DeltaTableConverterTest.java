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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.entity.ForeignTableEntity;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncResult;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.Test;

public class DeltaTableConverterTest {
  private final DeltaTableConverter converter;

  public DeltaTableConverterTest() {
    this.converter = new DeltaTableConverter();
  }

  @Test
  void testRunSync() {
    // This test is currently hardcoded and requires manually creating the Delta table using Spark
    // in the base location. Since this is for Snowvation, will automate these tests when bandwidth
    // allows.
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("db1", "schema1"), "people");
    final String baseLocation = "file:///tmp/delta-dataset/people/";
    final String metadataLocation = baseLocation + "metadata/v1.metadata.json";

    ForeignTableEntity entity =
        new ForeignTableEntity.Builder(identifier, metadataLocation)
            .setSource(TableFormat.DELTA)
            .setBaseLocation(baseLocation)
            .setName(identifier.name())
            .build();
    SyncResult syncResult = converter.runSync(entity);
    Assumptions.assumeThat(syncResult.getStatus()).isEqualTo(SyncResult.SyncStatus.SUCCESS);
  }
}
