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

package org.apache.polaris.storage.files.impl;

import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.storage.files.api.FileFilter;
import org.apache.polaris.storage.files.api.FileSpec;
import org.apache.polaris.storage.files.api.PurgeSpec;
import org.apache.polaris.storage.files.api.PurgeStats;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link FileOperationsImpl} that run against real Iceberg {@code FileIO}
 * implementations (S3, GCS, ADLS) via testcontainers.
 *
 * <p>The bulk of {@code FileOperationsImpl}'s logic — request batching, Iceberg-table file
 * identification, prefix listing semantics in the abstract — is covered by the unit-tier {@code
 * TestFileOperationsImpl} against an in-process {@code MockFileIO}. This tier only carries tests
 * whose assertions genuinely depend on the behaviour of the real cloud SDKs:
 *
 * <ul>
 *   <li>{@link #singleFileRoundTrip()} — smoke test that {@link FileOperationsImpl} correctly
 *       drives the real {@code FileIO}'s write / prefix-list / bulk-delete contracts end-to-end.
 *       Exists to catch protocol-level regressions from Iceberg SDK upgrades that an in-process
 *       mock cannot observe.
 *   <li>{@link #batchDeleteNonExistentFiles()} — asserts a real-SDK semantic that cannot honestly
 *       be mocked: {@code S3FileIO}, {@code GCSFileIO}, and {@code ADLSFileIO} all treat
 *       bulk-delete of a non-existent key as a successful no-op rather than raising {@code
 *       BulkDeletionFailureException}, and {@link FileOperationsImpl} must remain correct under
 *       that semantic.
 * </ul>
 */
public abstract class BaseITFileOperationsImpl extends BaseFileOperationsImpl {

  @Test
  public void singleFileRoundTrip() throws Exception {
    try (var fileIO = initializedFileIO()) {
      var prefix = prefix() + "singleFileRoundTrip/";
      var path = prefix + "hello";

      write(fileIO, path, new byte[] {1, 2, 3});

      var fileOps = new FileOperationsImpl(fileIO);

      try (Stream<FileSpec> listed = fileOps.findFiles(prefix, FileFilter.alwaysTrue())) {
        soft.assertThat(listed).extracting(FileSpec::location).containsExactly(path);
      }

      var stats = fileOps.purge(Stream.of(fileSpecFromLocation(path)), PurgeSpec.DEFAULT_INSTANCE);
      soft.assertThat(stats)
          .extracting(PurgeStats::purgeFileRequests, PurgeStats::failedFilePurges)
          .containsExactly(1L, 0L);

      try (Stream<FileSpec> listed = fileOps.findFiles(prefix, FileFilter.alwaysTrue())) {
        soft.assertThat(listed).isEmpty();
      }
    }
  }

  /** Verify that batch-deletions do not fail in case some files do not exist. */
  @Test
  public void batchDeleteNonExistentFiles() throws Exception {
    try (var fileIO = initializedFileIO()) {
      var prefix = prefix() + "batchDeleteNonExistentFiles/";

      write(fileIO, prefix + "1", new byte[1]);
      write(fileIO, prefix + "3", new byte[1]);
      write(fileIO, prefix + "5", new byte[1]);

      var fileOps = new FileOperationsImpl(fileIO);
      var result =
          fileOps.purge(
              IntStream.range(0, 10)
                  .mapToObj(i -> prefix + i)
                  .map(BaseFileOperationsImpl::fileSpecFromLocation),
              PurgeSpec.DEFAULT_INSTANCE);
      soft.assertThat(result)
          .extracting(PurgeStats::purgeFileRequests, PurgeStats::failedFilePurges)
          // Iceberg does not yield the correct number of purged files, 3/7 in this test (via
          // `BulkDeletionFailureException`) in case those do not exist.
          .containsExactly(10L, 0L);

      result =
          fileOps.purge(
              IntStream.range(20, 40)
                  .mapToObj(i -> prefix + i)
                  .map(BaseFileOperationsImpl::fileSpecFromLocation),
              PurgeSpec.DEFAULT_INSTANCE);
      soft.assertThat(result)
          .extracting(PurgeStats::purgeFileRequests, PurgeStats::failedFilePurges)
          // Iceberg does not yield the correct number of purged files, 0/20 in this test (via
          // `BulkDeletionFailureException`) in case those do not exist.
          .containsExactly(20L, 0L);

      result =
          fileOps.purge(
              IntStream.range(40, 60)
                  .mapToObj(i -> prefix + "40")
                  .map(BaseFileOperationsImpl::fileSpecFromLocation),
              PurgeSpec.DEFAULT_INSTANCE);
      soft.assertThat(result)
          .extracting(PurgeStats::purgeFileRequests, PurgeStats::failedFilePurges)
          // Iceberg does not yield the correct number of purged files, 0/1 in this test (via
          // `BulkDeletionFailureException`) in case those do not exist.
          .containsExactly(1L, 0L);
    }
  }

  public FileIO initializedFileIO() {
    var fileIO = createFileIO();
    fileIO.initialize(icebergProperties());
    return fileIO;
  }

  protected abstract FileIO createFileIO();

  protected abstract Map<String, String> icebergProperties();
}
