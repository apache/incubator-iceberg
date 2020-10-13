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

package org.apache.iceberg.hive;

public class MetastoreUtil {

  // this class is unique to Hive3 and cannot be found in Hive2, therefore a good proxy to see if
  // we are working against Hive3 dependencies
  private static final String HIVE3_UNIQUE_CLASS = "org.apache.hadoop.hive.serde2.io.DateWritableV2";

  private static final boolean HIVE3_PRESENT_ON_CLASSPATH = detectHive3();

  private MetastoreUtil() {
  }

  /**
   * Returns true if Hive3 dependencies are found on classpath, false otherwise.
   */
  public static boolean hive3PresentOnClasspath() {
    return HIVE3_PRESENT_ON_CLASSPATH;
  }

  private static boolean detectHive3() {
    try {
      Class.forName(HIVE3_UNIQUE_CLASS);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
