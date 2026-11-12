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
package org.apache.beam.sdk.values.variant;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses the JSON paths accepted by {@link Variant#get(String)}, using the same syntax as Spark's
 * {@code variant_get}.
 *
 * <p>Grammar: {@code $} followed by any number of steps, where a step is {@code .name}, {@code
 * ['name']}, {@code ["name"]} or {@code [index]}. Inside quotes, a backslash escapes the next
 * character.
 */
final class VariantPath {
  private VariantPath() {}

  /**
   * Returns the steps of {@code path}: a {@link String} per field and an {@link Integer} per index.
   */
  static List<Object> parse(String path) {
    if (path.isEmpty() || path.charAt(0) != '$') {
      throw invalid(path, "a path must start with '$'");
    }
    List<Object> steps = new ArrayList<>();
    int i = 1;
    int length = path.length();
    while (i < length) {
      char c = path.charAt(i);
      if (c == '.') {
        int start = ++i;
        while (i < length && path.charAt(i) != '.' && path.charAt(i) != '[') {
          i++;
        }
        if (i == start) {
          throw invalid(path, "empty field name at position " + start);
        }
        steps.add(path.substring(start, i));
      } else if (c == '[') {
        i++;
        if (i >= length) {
          throw invalid(path, "unterminated '['");
        }
        char quote = path.charAt(i);
        if (quote == '\'' || quote == '"') {
          StringBuilder name = new StringBuilder();
          i++;
          while (true) {
            if (i >= length) {
              throw invalid(path, "unterminated quoted field name");
            }
            char ch = path.charAt(i++);
            if (ch == '\\') {
              if (i >= length) {
                throw invalid(path, "dangling escape character");
              }
              name.append(path.charAt(i++));
            } else if (ch == quote) {
              break;
            } else {
              name.append(ch);
            }
          }
          if (i >= length || path.charAt(i) != ']') {
            throw invalid(path, "expected ']' after quoted field name");
          }
          i++;
          steps.add(name.toString());
        } else {
          int start = i;
          while (i < length && path.charAt(i) >= '0' && path.charAt(i) <= '9') {
            i++;
          }
          if (i == start || i >= length || path.charAt(i) != ']') {
            throw invalid(path, "expected an array index at position " + start);
          }
          try {
            steps.add(Integer.parseInt(path.substring(start, i)));
          } catch (NumberFormatException e) {
            throw invalid(path, "array index is too large");
          }
          i++;
        }
      } else {
        throw invalid(path, "unexpected character '" + c + "' at position " + i);
      }
    }
    return steps;
  }

  private static IllegalArgumentException invalid(String path, String reason) {
    return new IllegalArgumentException("Invalid variant path '" + path + "': " + reason);
  }
}
