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

package org.apache.iceberg;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * This class defines different metrics modes, which allow users to control the collection of
 * value_counts, null_value_counts, nan_value_counts, lower_bounds, upper_bounds for different columns in metadata.
 */
public class MetricsModes {

  private static final Pattern TRUNCATE = Pattern.compile("truncate\\((\\d+)\\)");

  private MetricsModes() {
  }

  public static MetricsMode fromString(String mode) {
    if ("none".equalsIgnoreCase(mode)) {
      return None.get();
    } else if ("counts".equalsIgnoreCase(mode)) {
      return Counts.get();
    } else if ("full".equalsIgnoreCase(mode)) {
      return Full.get();
    }

    Matcher truncateMatcher = TRUNCATE.matcher(mode.toLowerCase(Locale.ENGLISH));
    if (truncateMatcher.matches()) {
      int length = Integer.parseInt(truncateMatcher.group(1));
      return Truncate.withLength(length);
    }

    throw new IllegalArgumentException("Invalid metrics mode: " + mode);
  }

  public interface MetricsMode extends Serializable {
  }

  /**
   * Under this mode, value_counts, null_value_counts, nan_value_counts, lower_bounds, upper_bounds are not persisted.
   */
  public static class None extends ProxySerializableMetricsMode {
    private static final None INSTANCE = new None();

    public static None get() {
      return INSTANCE;
    }

    @Override
    public String toString() {
      return "none";
    }
  }

  /**
   * Under this mode, only value_counts, null_value_counts, nan_value_counts are persisted.
   */
  public static class Counts extends ProxySerializableMetricsMode {
    private static final Counts INSTANCE = new Counts();

    public static Counts get() {
      return INSTANCE;
    }

    @Override
    public String toString() {
      return "counts";
    }
  }

  /**
   * Under this mode, value_counts, null_value_counts, nan_value_counts
   * and truncated lower_bounds, upper_bounds are persisted.
   */
  public static class Truncate extends ProxySerializableMetricsMode {
    private final int length;

    private Truncate(int length) {
      this.length = length;
    }

    public static Truncate withLength(int length) {
      Preconditions.checkArgument(length > 0, "Truncate length should be positive");
      return new Truncate(length);
    }

    public int length() {
      return length;
    }

    @Override
    public String toString() {
      return String.format("truncate(%d)", length);
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      } else if (!(other instanceof Truncate)) {
        return false;
      }
      Truncate truncate = (Truncate) other;
      return length == truncate.length;
    }

    @Override
    public int hashCode() {
      return Objects.hash(length);
    }
  }

  /**
   * Under this mode, value_counts, null_value_counts, nan_value_counts
   * and full lower_bounds, upper_bounds are persisted.
   */
  public static class Full extends ProxySerializableMetricsMode {
    private static final Full INSTANCE = new Full();

    public static Full get() {
      return INSTANCE;
    }

    @Override
    public String toString() {
      return "full";
    }
  }

  /**
   * Auto promote sorted columns to truncate(16) if default is set at Counts or None.
   * @param defaultMode default mode
   * @return mode to use
   */
  public static MetricsMode promoteSortedColumnDefault(MetricsMode defaultMode) {
    if (defaultMode == None.get() || defaultMode == Counts.get()) {
      return Truncate.withLength(16);
    } else {
      return defaultMode;
    }
  }

  // we cannot serialize/deserialize MetricsMode directly as it breaks reference equality used in metrics utils
  private abstract static class ProxySerializableMetricsMode implements MetricsMode {
    Object writeReplace() throws ObjectStreamException {
      return new MetricsModeProxy(toString());
    }
  }

  private static class MetricsModeProxy implements Serializable {
    private String modeAsString;

    MetricsModeProxy(String modeAsString) {
      this.modeAsString = modeAsString;
    }

    Object readResolve() throws ObjectStreamException {
      return MetricsModes.fromString(modeAsString);
    }
  }
}
