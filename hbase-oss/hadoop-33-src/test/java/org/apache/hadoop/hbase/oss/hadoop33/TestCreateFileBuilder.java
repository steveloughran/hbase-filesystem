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

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataOutputStreamBuilder;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.oss.HBaseObjectStoreSemantics;
import org.apache.hadoop.hbase.oss.HBaseObjectStoreSemanticsTest;
import org.apache.hadoop.hbase.oss.sync.AutoLock;

import static org.apache.hadoop.hbase.oss.Constants.HBOSS_CLASSNAME;
import static org.apache.hadoop.hbase.oss.Constants.HBOSS_HADOOP33_CLASSNAME;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * test the createFile() builder API, where the existence checks
 * take place in build(), not the FS API call.
 * This means the lock checking needs to be postponed.
 */
public class TestCreateFileBuilder extends HBaseObjectStoreSemanticsTest {
  private static final Logger LOG =
        LoggerFactory.getLogger(TestCreateFileBuilder.class);

  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.set(HBOSS_CLASSNAME, HBOSS_HADOOP33_CLASSNAME);
    return conf;
  }



  @Test
  public void testCreateOverlappingBuilders() throws Exception {
    Path path = testPath("testCreateOverlappingBuilders");

    FSDataOutputStream out = null;
    try {
      FSDataOutputStreamBuilder builder1 = hboss.createFile(path)
          .overwrite(false);
      FSDataOutputStreamBuilder builder2 = hboss.createFile(path)
          .overwrite(false);
      // build the second of these.
      // even before the stream is closed, the first builder's build
      // call must fail.

      out = builder2.build();
      Assertions.assertThat(out)
          .describedAs("expected a LockedFSDataOutputStream")
          .isInstanceOf(AutoLock.LockedFSDataOutputStream.class);
      LOG.debug("building stream for  {}:", path);
      out.write(0);


      intercept(FileAlreadyExistsException.class, () ->
          builder1.build());
    } finally {
      if (out != null) {
        out.close();
      }
      hboss.delete(path, false);
    }
  }

}
