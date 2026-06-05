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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;

/**
 * Lightweight {@link FileIO} test double for unit tests of {@link FileOperationsImpl}.
 *
 * <p>This is intentionally a plain Java class, not an HTTP-layer mock. The unit tests do not need
 * to exercise S3/GCS/ADLS protocol details &mdash; they only need an Iceberg {@link FileIO} that
 * supports the prefix and bulk operations consumed by {@link FileOperationsImpl}. End-to-end
 * protocol behavior is covered by the {@code intTest} suite via {@code
 * polaris-minio-testcontainer}, {@code polaris-azurite-testcontainer}, and {@code
 * polaris-gcs-testcontainer}.
 *
 * <p>Two listing modes are supported:
 *
 * <ul>
 *   <li><b>Heap-backed</b> (default constructor): files are stored in an in-memory {@link
 *       ConcurrentSkipListMap}. {@link #listPrefix(String)} returns entries whose key starts with
 *       the requested prefix.
 *   <li><b>Synthetic</b> ({@link #withSyntheticListing(Function)}): {@link #listPrefix(String)}
 *       delegates to the supplied generator. Useful for tests that need to enumerate very large
 *       numbers of objects without materializing them.
 * </ul>
 *
 * <p>Hooks for fault injection are intentionally minimal:
 *
 * <ul>
 *   <li>{@link #onDeleteFiles(Consumer)} can throw {@link BulkDeletionFailureException} to drive
 *       the corresponding catch path in {@link FileOperationsImpl.PurgeBatcher#flush()}.
 * </ul>
 */
public final class MockFileIO implements FileIO, SupportsPrefixOperations, SupportsBulkOperations {

  private final NavigableMap<String, byte[]> heap = new ConcurrentSkipListMap<>();
  private final Set<String> deletedPaths = ConcurrentHashMap.newKeySet();
  private final Function<String, Stream<FileInfo>> prefixLister;

  private volatile Consumer<List<String>> deleteFilesHook = batch -> {};

  /** Heap-backed instance. */
  public MockFileIO() {
    this.prefixLister = null;
  }

  private MockFileIO(Function<String, Stream<FileInfo>> prefixLister) {
    this.prefixLister = prefixLister;
  }

  /**
   * Returns an instance whose {@link #listPrefix(String)} delegates to {@code prefixLister} instead
   * of the backing heap.
   */
  public static MockFileIO withSyntheticListing(Function<String, Stream<FileInfo>> prefixLister) {
    return new MockFileIO(prefixLister);
  }

  /** Pre-populate a file in the heap. */
  public void seed(String path, byte[] bytes) {
    heap.put(path, bytes);
  }

  /** Direct access to the backing heap, for assertions. */
  public NavigableMap<String, byte[]> heap() {
    return heap;
  }

  /** Returns every path that was passed to {@link #deleteFile(String)} or {@link #deleteFiles}. */
  public Set<String> deletedPaths() {
    return deletedPaths;
  }

  /**
   * Install a hook that runs before each bulk delete batch. The hook may throw {@link
   * BulkDeletionFailureException} to drive {@link FileOperationsImpl}'s failure handling.
   */
  public void onDeleteFiles(Consumer<List<String>> hook) {
    this.deleteFilesHook = hook;
  }

  @Override
  public InputFile newInputFile(String path) {
    var bytes = heap.get(path);
    if (bytes == null) {
      throw new NotFoundException("Mock file not found: %s", path);
    }
    return new InMemoryInputFile(path, bytes);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new MockOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    deletedPaths.add(path);
    heap.remove(path);
  }

  @Override
  public void deleteFiles(Iterable<String> paths) throws BulkDeletionFailureException {
    var batch = new ArrayList<String>();
    for (var path : paths) {
      batch.add(path);
    }
    deleteFilesHook.accept(batch);
    for (var path : batch) {
      deletedPaths.add(path);
      heap.remove(path);
    }
  }

  @Override
  public Iterable<FileInfo> listPrefix(String prefix) {
    if (prefixLister != null) {
      return () -> prefixLister.apply(prefix).iterator();
    }
    return () ->
        heap.subMap(prefix, true, prefix + Character.MAX_VALUE, true).entrySet().stream()
            .filter(e -> e.getKey().startsWith(prefix))
            .map(e -> new FileInfo(e.getKey(), e.getValue().length, 0L))
            .iterator();
  }

  @Override
  public void deletePrefix(String prefix) {
    Set.copyOf(heap.subMap(prefix, true, prefix + Character.MAX_VALUE, true).keySet())
        .forEach(this::deleteFile);
  }

  @Override
  public Map<String, String> properties() {
    return Map.of();
  }

  @Override
  public void initialize(Map<String, String> properties) {}

  @Override
  public void close() {
    heap.clear();
  }

  private final class MockOutputFile implements OutputFile {
    private final String path;

    MockOutputFile(String path) {
      this.path = path;
    }

    @Override
    public PositionOutputStream create() {
      if (heap.containsKey(path)) {
        throw new AlreadyExistsException("File already exists: %s", path);
      }
      return createOrOverwrite();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      return new PositionOutputStream() {
        private long pos = 0L;
        private boolean closed = false;

        @Override
        public long getPos() {
          return pos;
        }

        @Override
        public void write(int b) {
          buffer.write(b);
          pos++;
        }

        @Override
        public void write(byte[] b, int off, int len) {
          buffer.write(b, off, len);
          pos += len;
        }

        @Override
        public void close() throws IOException {
          if (closed) {
            return;
          }
          closed = true;
          super.close();
          heap.put(path, buffer.toByteArray());
        }
      };
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public InputFile toInputFile() {
      return newInputFile(path);
    }
  }
}
