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

import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.sync.TreeLockManager;

/**
 * An input stream builder which locks the path to read on the build() call.
 * all builder operations must return this instance so that the final
 * build will acquire the lock.
 */
public class LockedOpenFileBuilder extends
    LockingFSBuilderWrapper<CompletableFuture<FSDataInputStream>, FutureDataInputStreamBuilder>
    implements FutureDataInputStreamBuilder {

  public LockedOpenFileBuilder(@Nonnull final Path path,
      final TreeLockManager sync,
      final FutureDataInputStreamBuilder wrapped) {
    super(path, sync, wrapped, x -> x);
  }

  public FutureDataInputStreamBuilder withFileStatus(final FileStatus status) {
    getWrapped().withFileStatus(status);
    return getThisBuilder();
  }

}
