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
import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathHandle;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hbase.oss.HBaseObjectStoreSemantics;

import static org.apache.hadoop.hbase.oss.Constants.CAPABILITY_HBOSS;
import static org.apache.hadoop.hbase.oss.Constants.CAPABILITY_HBOSS33;

/**
 * Add the hadoop-3.3+ builder methods.
 */
public class HBase33ObjectStoreSemantics extends HBaseObjectStoreSemantics {
  private static final Logger LOG =
        LoggerFactory.getLogger(HBase33ObjectStoreSemantics.class);

  @Override
  public void initialize(final URI name, final Configuration conf) throws IOException {
    super.initialize(name, conf);
    LOG.info("Using HBase33ObjectStoreSemantics for hadoop 3.3 APIs");
  }

  @Override
  public FutureDataInputStreamBuilder openFile(final Path path)
      throws IOException, UnsupportedOperationException {
    return new LockedOpenFileBuilder(
        path,
        getSync(),
        fs.openFile(path));
  }

  /**
   * This is rarely supported, and as there's no way to
   * get the path from a pathHandle, impossible to lock.
   * @param pathHandle path
   * @return never returns successfully.
   * @throws UnsupportedOperationException always
   */
  @Override
  public FutureDataInputStreamBuilder openFile(final PathHandle pathHandle)
      throws IOException, UnsupportedOperationException {
    throw new UnsupportedOperationException("openFile(PathHandle) unsupported");
  }

  @Override
  public FSDataOutputStreamBuilder createFile(final Path path) {
    return new LockedCreateFileBuilder(this,
        path,
        getSync(),
        super.createFile(path));
  }

  @Override
  public boolean hasCapability(final String capability) {
    if (CAPABILITY_HBOSS33.equals(capability)) {
      return true;
    }
    return super.hasCapability(capability);
  }
}
