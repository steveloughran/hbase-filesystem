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

package org.apache.hadoop.hbase.oss.hadoop33;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.HBaseObjectStoreSemantics;
import org.apache.hadoop.hbase.oss.sync.AutoLock;
import org.apache.hadoop.hbase.oss.sync.TreeLockManager;

/**
 * An output stream builder for createFile().
 */
@SuppressWarnings("rawtypes")
public class LockedCreateFileBuilder
    extends FSDataOutputStreamBuilder<FSDataOutputStream, LockedCreateFileBuilder> {
  private static final Logger LOG =
      LoggerFactory.getLogger(LockedCreateFileBuilder.class);

  private final Path path;
  private final TreeLockManager sync;
  private final FSDataOutputStreamBuilder wrapped;

  public LockedCreateFileBuilder(@Nonnull final FileSystem fileSystem,
      final Path path,
      final TreeLockManager sync,
      final FSDataOutputStreamBuilder wrapped) {
    super(fileSystem, path);
    this.path = path;
    this.sync = sync;
    this.wrapped = wrapped;
  }

  @Override
  public FSDataOutputStream build() throws IOException {
    LOG.debug("Building output stream for {}", path);
    AutoLock lock = sync.lockWrite(path);
    try {
      FSDataOutputStream stream = wrapped.build();
      return new AutoLock.LockedFSDataOutputStream(stream, lock);
    } catch (IOException e) {
      lock.close();
      throw e;
    }
  }

  @Override
  public LockedCreateFileBuilder getThisBuilder() {
    return this;
  }

  @Override
  public LockedCreateFileBuilder opt(@Nonnull final String key,
      @Nonnull final String value) {
    wrapped.opt(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder opt(@Nonnull final String key, final boolean value) {
    wrapped.opt(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder opt(@Nonnull final String key, final int value) {
    wrapped.opt(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder opt(@Nonnull final String key, final float value) {
    wrapped.opt(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder opt(@Nonnull final String key, final double value) {
    wrapped.opt(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder opt(@Nonnull final String key,
      @Nonnull final String... values) {
    wrapped.opt(key, values);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder must(@Nonnull final String key,
      @Nonnull final String value) {
    wrapped.must(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder must(@Nonnull final String key, final boolean value) {
    wrapped.must(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder must(@Nonnull final String key, final int value) {
    wrapped.must(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder must(@Nonnull final String key, final float value) {
    wrapped.must(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder must(@Nonnull final String key, final double value) {
    wrapped.must(key, value);
    return getThisBuilder();
  }

  @Override
  public LockedCreateFileBuilder must(@Nonnull final String key,
      @Nonnull final String... values) {
    wrapped.must(key, values);
    return getThisBuilder();
  }

  /**
   * Configure with a long value.
   * getThisBuilder() is not on the original interface.
   * It is implemented in the wrapper by converting
   * to a string and calling the wrapper's
   * {@code #opt(String, String)}.
   */
  public LockedCreateFileBuilder opt(@Nonnull String key, long value) {
    wrapped.opt(key, Long.toString(value));
    return getThisBuilder();
  }

  /**
   * Configure with a long value.
   * This is not on the original interface.
   * It is implemented in the wrapper by converting
   * to a string and calling the wrapper's
   * {@code #must(String, String)}.
   */
  public LockedCreateFileBuilder must(@Nonnull String key, long value) {
    wrapped.must(key, Long.toString(value));
    return getThisBuilder();
  }

}
