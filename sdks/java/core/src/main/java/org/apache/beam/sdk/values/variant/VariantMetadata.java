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

import static org.apache.beam.sdk.values.variant.VariantUtil.MAX_SIZE;
import static org.apache.beam.sdk.values.variant.VariantUtil.SORTED_STRINGS_BIT;
import static org.apache.beam.sdk.values.variant.VariantUtil.VERSION;
import static org.apache.beam.sdk.values.variant.VariantUtil.VERSION_MASK;
import static org.apache.beam.sdk.values.variant.VariantUtil.checkBounds;
import static org.apache.beam.sdk.values.variant.VariantUtil.malformed;
import static org.apache.beam.sdk.values.variant.VariantUtil.readUnsigned;

import java.nio.charset.StandardCharsets;

/**
 * A read-only view of a Variant metadata buffer: the dictionary of field names shared by every
 * object inside one Variant value.
 *
 * <p>The structure is validated when the view is created, so later lookups only need to check the
 * field ID. Names are compared as raw UTF-8 bytes and decoded to {@link String} only on demand.
 */
final class VariantMetadata {
  private final byte[] bytes;
  private final boolean sorted;
  private final int offsetSize;
  private final int dictionarySize;
  private final int offsetsStart;
  private final int stringsStart;

  VariantMetadata(byte[] bytes) {
    if (bytes.length == 0) {
      throw malformed("metadata is empty");
    }
    if (bytes.length > MAX_SIZE) {
      throw malformed("metadata is larger than " + MAX_SIZE + " bytes");
    }
    int header = bytes[0] & 0xFF;
    int version = header & VERSION_MASK;
    if (version != VERSION) {
      throw new IllegalArgumentException("Unsupported variant metadata version: " + version);
    }
    this.bytes = bytes;
    this.sorted = (header & SORTED_STRINGS_BIT) != 0;
    this.offsetSize = ((header >> 6) & 0x3) + 1;
    long size = readUnsigned(bytes, 1, offsetSize);
    this.offsetsStart = 1 + offsetSize;
    long stringsStartPos = offsetsStart + (size + 1) * offsetSize;
    checkBounds(bytes, 0, stringsStartPos, "metadata dictionary offsets");
    this.dictionarySize = (int) size;
    this.stringsStart = (int) stringsStartPos;

    // Offsets must be non-decreasing and stay inside the string bytes.
    int stringsLength = bytes.length - stringsStart;
    long previous = 0;
    for (int i = 0; i <= dictionarySize; i++) {
      long offset = readUnsigned(bytes, offsetsStart + (long) i * offsetSize, offsetSize);
      if (offset < previous || offset > stringsLength) {
        throw malformed("metadata offset " + i + " is invalid");
      }
      previous = offset;
    }
  }

  /** Returns the raw metadata bytes. Callers must not modify the returned array. */
  byte[] bytes() {
    return bytes;
  }

  int size() {
    return dictionarySize;
  }

  boolean isSorted() {
    return sorted;
  }

  /** Returns the field name with the given dictionary ID. */
  String get(int id) {
    int start = stringStart(id);
    return new String(bytes, start, stringEnd(id) - start, StandardCharsets.UTF_8);
  }

  /** Compares the name with the given ID to {@code key} (UTF-8) in unsigned byte order. */
  int compare(int id, byte[] key) {
    int start = stringStart(id);
    return VariantUtil.compareUtf8(bytes, start, stringEnd(id) - start, key);
  }

  private int stringStart(int id) {
    checkId(id);
    return stringsStart
        + (int) readUnsigned(bytes, offsetsStart + (long) id * offsetSize, offsetSize);
  }

  private int stringEnd(int id) {
    return stringsStart
        + (int) readUnsigned(bytes, offsetsStart + (long) (id + 1) * offsetSize, offsetSize);
  }

  private void checkId(int id) {
    if (id < 0 || id >= dictionarySize) {
      throw malformed(
          "field id " + id + " is outside the metadata dictionary of size " + dictionarySize);
    }
  }
}
