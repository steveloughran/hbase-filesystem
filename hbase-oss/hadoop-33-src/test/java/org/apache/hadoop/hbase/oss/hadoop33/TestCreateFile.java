/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.oss.hadoop33;

import org.junit.Test;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.HBaseObjectStoreSemanticsTest;
import org.apache.hadoop.hbase.oss.sync.AutoLock;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.Assert.assertTrue;

/**
 * test the createFile() builder API, where the existence checks
 * take place in build()
 */
public class TestCreateFile extends HBaseObjectStoreSemanticsTest {

  @Test
  public void testCreateOverlappingBuilders() throws Exception {
    Path serialPath = testPath("testCreateNonRecursiveSerial");
    try {
      FSDataOutputStreamBuilder builder1 = hboss.createFile(serialPath)
          .overwrite(false);
      FSDataOutputStreamBuilder builder2 = hboss.createFile(serialPath)
          .overwrite(false);
      // build the second of these.
      // even before the stream is closed, the first builder's build
      // call must fail.
      FSDataOutputStream out;
      out = builder2.build();
      assertTrue("Not a LockedFSDataOutputStream: " + out,
          out instanceof AutoLock.LockedFSDataOutputStream);

      out.write(0);

      intercept(FileAlreadyExistsException.class, () ->
          builder1.build());
    } finally {
      hboss.delete(serialPath);
    }
  }

}
