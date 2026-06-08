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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileInfo;
import org.apache.polaris.storage.files.api.FileFilter;
import org.apache.polaris.storage.files.api.FileSpec;
import org.apache.polaris.storage.files.api.PurgeSpec;
import org.apache.polaris.storage.files.api.PurgeStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link FileOperationsImpl} that drive the implementation through a plain
 * in-process {@link MockFileIO}.
 *
 * <p>These tests cover the same {@link FileOperationsImpl} scenarios as PR&nbsp;#3256's {@code
 * BaseTestFileOperationsImpl} (the small/huge {@code purgeIcebergTable}, {@code someFiles}, {@code
 * manyFiles}, and {@code icebergIntegration} cases) without depending on an HTTP-layer mock. The
 * integration tier ({@code BaseITFileOperationsImpl}) continues to exercise the same code against
 * real S3/GCS/ADLS FileIO implementations via {@code polaris-minio-testcontainer}, {@code
 * polaris-azurite-testcontainer}, and {@code polaris-gcs-testcontainer}, which is where
 * backend-specific behavior such as the per- implementation bulk-delete batch limit in {@link
 * FileOperationsImpl#implSpecificDeleteBatchLimit} belongs.
 */
class TestFileOperationsImpl extends BaseFileOperationsImpl {

  private static final String PREFIX = "mock://bucket/";

  @Override
  protected String prefix() {
    return PREFIX;
  }

  // ---------------------------------------------------------------------------
  // purgeIcebergTable — exercises the metadata-read path plus batched bulk delete,
  // parameterized over (numSnapshots, numManifestFiles, numDataFiles, deleteBatchSize)
  // to drive multiple PurgeBatcher flushes per run.
  // ---------------------------------------------------------------------------

  @ParameterizedTest
  @MethodSource
  public void purgeIcebergTable(
      int numSnapshots, int numManifestFiles, int numDataFiles, int deleteBatchSize)
      throws Exception {
    doPurgeIcebergTable(numSnapshots, numManifestFiles, numDataFiles, deleteBatchSize);
  }

  @ParameterizedTest
  @MethodSource
  public void purgeIcebergTableHuge(
      int numSnapshots, int numManifestFiles, int numDataFiles, int deleteBatchSize)
      throws Exception {
    doPurgeIcebergTable(numSnapshots, numManifestFiles, numDataFiles, deleteBatchSize);
  }

  static Stream<Arguments> purgeIcebergTable() {
    return Stream.of(
        // one snapshot, three manifest files, 5 data files per manifest file
        Arguments.of(1, 3, 5, 10),
        // five snapshot, 7 manifest files per snapshot, 13 data files per manifest file
        Arguments.of(5, 7, 13, 10));
  }

  static Stream<Arguments> purgeIcebergTableHuge() {
    return Stream.of(
        // five snapshot, 7 manifest files per snapshot, 1000 data files per manifest file
        // -> 35,041 total file purge requests
        Arguments.of(5, 7, 1_000, 500));
  }

  private void doPurgeIcebergTable(
      int numSnapshots, int numManifestFiles, int numDataFiles, int deleteBatchSize)
      throws Exception {
    var fixtures = new IcebergFixtures(PREFIX, numSnapshots, numManifestFiles, numDataFiles);
    var tableMetadataLocation = PREFIX + "foo.metadata.json";

    try (var fileIO = new MockFileIO()) {
      // Pre-populate the heap with the metadata + manifest list + manifest file bytes.
      // Data files themselves are never read by FileOperationsImpl — only enumerated by the
      // manifests and then issued for bulk delete — so they do not need to be present; the
      // assertions below verify the bulk-delete *requests* directly.
      fileIO.seed(tableMetadataLocation, fixtures.tableMetadataBytes);
      var expectedDeletes = new HashSet<String>();
      expectedDeletes.add(tableMetadataLocation);
      for (int snapshotId = 1; snapshotId <= fixtures.numSnapshots; snapshotId++) {
        var manifestListPath = fixtures.manifestListPath(snapshotId);
        fileIO.seed(manifestListPath, fixtures.serializedManifestList(snapshotId));
        expectedDeletes.add(manifestListPath);
        for (int mf = 0; mf < fixtures.numManifestFiles; mf++) {
          var manifestPath = fixtures.manifestFilePath(snapshotId, mf);
          fileIO.seed(manifestPath, fixtures.serializedManifestFile(snapshotId, mf, manifestPath));
          expectedDeletes.add(manifestPath);
          for (int df = 0; df < numDataFiles; df++) {
            // Same path format as IcebergFixtures#serializedManifestFile.
            expectedDeletes.add(
                format("%s%05d/%05d/%05d/data.parquet", PREFIX, snapshotId, mf, df));
          }
        }
      }

      var fileOps = new FileOperationsImpl(fileIO);
      PurgeStats purgeStats =
          fileOps.purgeIcebergTable(
              tableMetadataLocation,
              PurgeSpec.DEFAULT_INSTANCE.withDeleteBatchSize(deleteBatchSize));

      long expectedRequests = expectedPurgeRequests(numSnapshots, numManifestFiles, numDataFiles);
      soft.assertThat(purgeStats.purgeFileRequests()).isEqualTo(expectedRequests);
      soft.assertThat(purgeStats.failedFilePurges()).isZero();
      // Every path the implementation enumerated was issued to bulk delete.
      soft.assertThat(fileIO.deletedPaths()).hasSize((int) expectedRequests);
      soft.assertThat(fileIO.deletedPaths()).isEqualTo(expectedDeletes);
      // Everything that was actually present in the heap was removed.
      soft.assertThat(fileIO.heap()).isEmpty();
    }
  }

  private static long expectedPurgeRequests(
      int numSnapshots, int numManifestFiles, int numDataFiles) {
    // metadata + per-snapshot(manifest list + per-manifest(manifest file + data files))
    return 1L + numSnapshots * (1L + (long) numManifestFiles * (1L + numDataFiles));
  }

  // ---------------------------------------------------------------------------
  // someFiles — exercises findFiles + bulk delete against a finite heap.
  // ---------------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(ints = {500})
  public void someFiles(int numFiles) throws Exception {
    try (var fileIO = new MockFileIO()) {
      for (int i = 0; i < numFiles; i++) {
        fileIO.seed(format(PREFIX + "path/%d/%d", i % 100, i), new byte[0]);
      }

      var fileOps = new FileOperationsImpl(fileIO);

      try (Stream<FileSpec> files = fileOps.findFiles(PREFIX, FileFilter.alwaysTrue())) {
        assertThat(files).hasSize(numFiles);
      }

      int deletes = numFiles / 10;
      assertThat(
              fileOps.purge(
                  IntStream.range(0, deletes)
                      .mapToObj(i -> format(PREFIX + "path/%d/%d", i % 100, i))
                      .map(BaseFileOperationsImpl::fileSpecFromLocation),
                  PurgeSpec.DEFAULT_INSTANCE))
          .extracting(PurgeStats::purgeFileRequests)
          .isEqualTo((long) deletes);

      try (Stream<FileSpec> files = fileOps.findFiles(PREFIX, FileFilter.alwaysTrue())) {
        assertThat(files).hasSize(numFiles - deletes);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // findFiles relative- vs absolute-path handling. Iceberg's ADLSFileIO returns
  // *relative* paths from listPrefix; S3FileIO and GCSFileIO return absolute paths.
  // FileOperationsImpl.findFiles handles both by resolving relative entries against
  // the requested prefix. The ADLS branch is otherwise difficult to cover: Azurite
  // is incompatible with the ADLS v2 list-prefix REST endpoint, which is why
  // ITFileOperationsImplWithADLS#icebergIntegration is @Disabled. Driving the
  // branch directly through MockFileIO tests the Polaris contract without
  // depending on either a real cloud SDK or an ADLS HTTP emulator.
  // ---------------------------------------------------------------------------

  @Test
  public void findFilesPrependsPrefixForRelativeAdlsStyleListings() throws Exception {
    var relativePaths = List.of("alpha", "beta/one", "beta/two", "gamma/three/four");
    try (var fileIO =
        MockFileIO.withSyntheticListing(
            prefix -> relativePaths.stream().map(p -> new FileInfo(p, 0L, 0L)))) {

      var fileOps = new FileOperationsImpl(fileIO);
      try (Stream<FileSpec> files = fileOps.findFiles(PREFIX, FileFilter.alwaysTrue())) {
        assertThat(files)
            .extracting(FileSpec::location)
            .containsExactlyInAnyOrder(
                PREFIX + "alpha",
                PREFIX + "beta/one",
                PREFIX + "beta/two",
                PREFIX + "gamma/three/four");
      }
    }
  }

  @Test
  public void findFilesLeavesAbsoluteS3GcsStylePathsUnchanged() throws Exception {
    var absolutePaths = List.of(PREFIX + "alpha", PREFIX + "beta/one", PREFIX + "gamma/two");
    try (var fileIO =
        MockFileIO.withSyntheticListing(
            prefix -> absolutePaths.stream().map(p -> new FileInfo(p, 0L, 0L)))) {

      var fileOps = new FileOperationsImpl(fileIO);
      try (Stream<FileSpec> files = fileOps.findFiles(PREFIX, FileFilter.alwaysTrue())) {
        assertThat(files)
            .extracting(FileSpec::location)
            .containsExactlyInAnyOrderElementsOf(absolutePaths);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // manyFiles — exercises streaming findFiles against a synthetic listing. The
  // generator never materializes the objects.
  // ---------------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(ints = {50_000})
  public void manyFiles(int numFiles) throws Exception {
    var pathPrefix = "x".repeat(1000) + "/";

    try (var fileIO =
        MockFileIO.withSyntheticListing(
            prefix ->
                IntStream.range(0, numFiles)
                    .mapToObj(i -> new FileInfo(pathPrefix + format("%010d", i), 0L, 0L)))) {

      var fileOps = new FileOperationsImpl(fileIO);

      try (Stream<FileSpec> files = fileOps.findFiles(PREFIX, FileFilter.alwaysTrue())) {
        assertThat(files).hasSize(numFiles);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // bulkDeleteFailure — demonstrates that MockFileIO can drive the
  // BulkDeletionFailureException catch path in FileOperationsImpl.PurgeBatcher,
  // without an HTTP-layer mock.
  // ---------------------------------------------------------------------------

  @Test
  public void bulkDeleteFailurePartial() throws Exception {
    try (var fileIO = new MockFileIO()) {
      for (int i = 0; i < 20; i++) {
        fileIO.seed(format(PREFIX + "p/%d", i), new byte[0]);
      }
      // Report 2 failed objects per batch; FileOperationsImpl should accumulate them.
      fileIO.onDeleteFiles(
          batch -> {
            throw new BulkDeletionFailureException(2);
          });

      var fileOps = new FileOperationsImpl(fileIO);
      var stats =
          fileOps.purge(
              IntStream.range(0, 20)
                  .mapToObj(i -> format(PREFIX + "p/%d", i))
                  .map(BaseFileOperationsImpl::fileSpecFromLocation),
              PurgeSpec.DEFAULT_INSTANCE.withDeleteBatchSize(10));

      // Two batches of 10 each, with 2 failures per batch.
      assertThat(stats.purgeFileRequests()).isEqualTo(16L);
      assertThat(stats.failedFilePurges()).isEqualTo(4L);
    }
  }

  @Test
  public void newInputFileMissingMetadataReturnsNoFiles() throws Exception {
    try (var fileIO = new MockFileIO()) {
      var fileOps = new FileOperationsImpl(fileIO);
      // No metadata seeded — readTableMetadataFailsafe should swallow the NotFoundException
      // and identifyIcebergTableFiles should return an empty stream.
      assertThatThrownBy(() -> fileIO.newInputFile(PREFIX + "missing"))
          .isInstanceOf(NotFoundException.class);
      assertThat(fileOps.identifyIcebergTableFiles(PREFIX + "missing.metadata.json")).isEmpty();
    }
  }

  // ---------------------------------------------------------------------------
  // icebergIntegration — smoke test that builds a small Iceberg table via the test fixture
  // and runs identifyIcebergTableFiles + findFiles + purgeIcebergTable end-to-end against
  // a heap-backed MockFileIO. The intTest suite covers the same path against real backends.
  // ---------------------------------------------------------------------------

  @Test
  public void icebergIntegration() throws Exception {
    try (var fileIO = new MockFileIO()) {
      icebergIntegration(fileIO, Map.of());
    }
  }
}
