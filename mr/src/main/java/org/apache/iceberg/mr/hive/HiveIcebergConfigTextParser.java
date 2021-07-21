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

package org.apache.iceberg.mr.hive;

import java.util.Locale;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class HiveIcebergConfigTextParser {
  private static final int PARTITIONING_TEXT_MAX_LENGTH = 1000;

  private HiveIcebergConfigTextParser() {
  }

  public static PartitionSpec fromPartitioningText(Schema schema, String text) {
    Preconditions.checkArgument(text.length() < PARTITIONING_TEXT_MAX_LENGTH,
        "Partition spec text too long: max allowed length %s, but got %s",
        PARTITIONING_TEXT_MAX_LENGTH, text.length());
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);

    StringBuilder sb = new StringBuilder();
    boolean inTransform = false;
    boolean inEscape = false;
    boolean needBuild = false;
    String transformPart = null;
    String[] argParts = {null, null};
    int argIndex = 0;
    for (int i = 0; i < text.length(); i++) {
      char curr = text.charAt(i);
      if (needBuild) {
        Preconditions.checkArgument(curr == ' ' || curr == '|',
            "Cannot parse partitioning text: end of bracket not followed by ' ' or '|' at position %s in %s", i, text);
      }

      switch (curr) {
        case '(':
          Preconditions.checkArgument(!inTransform,
              "Cannot parse partitioning text: found multiple ( at position %s in %s", i, text);
          inTransform = true;
          transformPart = sb.toString();
          sb.delete(0, sb.length());
          break;
        case ')':
          inTransform = false;
          needBuild = true;
          break;
        case '`':
          inEscape = !inEscape;
          break;
        case ',':
          if (inEscape) {
            sb.append(curr);
          } else {
            Preconditions.checkArgument(argIndex == 0,
                "Cannot parse partitioning text, more than 2 transform arguments used at position %s in %s", i, text);
            argParts[argIndex] = sb.toString();
            sb.delete(0, sb.length());
            argIndex = 1;
          }
          break;
        case ' ':
          break;
        case '|':
          if (inEscape) {
            sb.append(curr);
          } else {
            argParts[argIndex] = sb.toString();
            addToPartitionSpec(builder, text, transformPart, argParts);
            sb.delete(0, sb.length());
            transformPart = null;
            argParts[0] = null;
            argParts[1] = null;
            argIndex = 0;
            needBuild = false;
          }
          break;
        default:
          sb.append(curr);
          break;
      }
    }

    argParts[argIndex] = sb.toString();
    addToPartitionSpec(builder, text, transformPart, argParts);
    return builder.build();
  }

  private static void addToPartitionSpec(PartitionSpec.Builder builder, String text,
                                         String transformPart, String[] argParts) {
    if (transformPart == null) {
      Preconditions.checkArgument(argParts[1] == null,
          "Cannot parse partitioning text: detected multiple args for identity transform in %s", text);
      builder.identity(argParts[0]);
    } else {
      if (argParts[1] == null) {
        switch (transformPart.toLowerCase(Locale.ENGLISH)) {
          case "year":
            builder.year(argParts[0]);
            break;
          case "month":
            builder.month(argParts[0]);
            break;
          case "day":
            builder.day(argParts[0]);
            break;
          case "hour":
            builder.hour(argParts[0]);
            break;
          case "alwaysnull":
            builder.alwaysNull(argParts[0]);
            break;
          default:
            throw new IllegalArgumentException("Cannot parse partitioning text: unknown 1-arg transform " +
                transformPart + " in " + text);
        }
      } else {
        switch (transformPart.toLowerCase(Locale.ENGLISH)) {
          case "bucket":
            builder.bucket(argParts[1], parseIntArg(text, transformPart, argParts[0]));
            break;
          case "truncate":
            builder.truncate(argParts[0], parseIntArg(text, transformPart, argParts[1]));
            break;
          default:
            throw new IllegalArgumentException("Cannot parse partitioning text: unknown 2-arg transform " +
                transformPart + " in " + text);
        }
      }
    }
  }

  private static int parseIntArg(String text, String transformPart, String arg) {
    try {
      return Integer.parseInt(arg);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Cannot parse partitioning text: " +
          "fail to parse integer argument in 2-arg transform " + transformPart + " in " + text);
    }
  }
}
