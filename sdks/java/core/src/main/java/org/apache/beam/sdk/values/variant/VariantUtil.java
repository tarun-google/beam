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

/**
 * Constants and low-level helpers for the Apache Parquet Variant binary encoding, version 1.
 *
 * <p>See <a href="https://github.com/apache/parquet-format/blob/master/VariantEncoding.md">
 * VariantEncoding.md</a> for the specification.
 */
final class VariantUtil {
  private VariantUtil() {}

  /** The only metadata version defined by the specification. */
  static final int VERSION = 1;

  static final int VERSION_MASK = 0x0F;
  static final int SORTED_STRINGS_BIT = 0x10;

  // Basic types, stored in the low 2 bits of a value header byte.
  static final int BASIC_TYPE_MASK = 0x03;
  static final int BASIC_TYPE_PRIMITIVE = 0;
  static final int BASIC_TYPE_SHORT_STRING = 1;
  static final int BASIC_TYPE_OBJECT = 2;
  static final int BASIC_TYPE_ARRAY = 3;

  // Primitive type IDs, stored in the high 6 bits of a primitive value header byte.
  static final int TYPE_NULL = 0;
  static final int TYPE_TRUE = 1;
  static final int TYPE_FALSE = 2;
  static final int TYPE_INT8 = 3;
  static final int TYPE_INT16 = 4;
  static final int TYPE_INT32 = 5;
  static final int TYPE_INT64 = 6;
  static final int TYPE_DOUBLE = 7;
  static final int TYPE_DECIMAL4 = 8;
  static final int TYPE_DECIMAL8 = 9;
  static final int TYPE_DECIMAL16 = 10;
  static final int TYPE_DATE = 11;
  static final int TYPE_TIMESTAMP = 12;
  static final int TYPE_TIMESTAMP_NTZ = 13;
  static final int TYPE_FLOAT = 14;
  static final int TYPE_BINARY = 15;
  static final int TYPE_STRING = 16;
  static final int TYPE_TIME = 17;
  static final int TYPE_TIMESTAMP_NANOS = 18;
  static final int TYPE_TIMESTAMP_NTZ_NANOS = 19;
  static final int TYPE_UUID = 20;

  static final int MAX_SHORT_STRING_BYTES = 63;
  static final int MAX_DECIMAL4_PRECISION = 9;
  static final int MAX_DECIMAL8_PRECISION = 18;
  static final int MAX_DECIMAL_PRECISION = 38;

  /**
   * Maximum number of nested objects and arrays accepted when building, parsing or walking a
   * Variant. This is a safety limit against stack exhaustion from untrusted input, not a spec
   * limit. It matches the limit used by parquet-java and Apache Iceberg, and Jackson's default
   * nesting limit.
   */
  static final int MAX_DEPTH = 1000;

  /**
   * Maximum size in bytes of a metadata or value buffer. This is a safety limit against memory
   * exhaustion from untrusted input, not a spec limit. It matches Apache Spark's limit so that
   * values written by Beam stay readable by Spark.
   */
  static final int MAX_SIZE = 16 * 1024 * 1024;

  /**
   * Throws if opening another object or array would exceed {@link #MAX_DEPTH} levels of nesting.
   * {@code depth} is the number of objects and arrays that already enclose the new one. This counts
   * nesting the same way Jackson does, so this check fires before Jackson's own limit.
   */
  static void checkNestingDepth(int depth) {
    if (depth >= MAX_DEPTH) {
      throw new IllegalArgumentException(
          "Variant nesting depth exceeds the maximum of " + MAX_DEPTH);
    }
  }

  static IllegalArgumentException malformed(String message) {
    return new IllegalArgumentException("Malformed variant: " + message);
  }

  /** Throws if {@code [pos, pos + length)} is not within {@code bytes}. */
  static void checkBounds(byte[] bytes, long pos, long length, String what) {
    if (pos < 0 || length < 0 || pos + length > bytes.length) {
      throw malformed(
          what
              + " is out of bounds (offset "
              + pos
              + ", length "
              + length
              + ", buffer size "
              + bytes.length
              + ")");
    }
  }

  /** Reads an unsigned little-endian integer of 1 to 4 bytes. */
  static long readUnsigned(byte[] bytes, long pos, int numBytes) {
    checkBounds(bytes, pos, numBytes, "unsigned integer");
    long result = 0;
    for (int i = 0; i < numBytes; i++) {
      result |= (bytes[(int) pos + i] & 0xFFL) << (8 * i);
    }
    return result;
  }

  /** Reads a signed (two's complement) little-endian integer of 1 to 8 bytes. */
  static long readSigned(byte[] bytes, long pos, int numBytes) {
    checkBounds(bytes, pos, numBytes, "signed integer");
    long result = 0;
    for (int i = 0; i < numBytes; i++) {
      result |= (bytes[(int) pos + i] & 0xFFL) << (8 * i);
    }
    int unusedBits = 64 - 8 * numBytes;
    return (result << unusedBits) >> unusedBits;
  }

  /** Writes the low {@code numBytes} bytes of {@code value} in little-endian order. */
  static void writeLittleEndian(byte[] bytes, int pos, long value, int numBytes) {
    for (int i = 0; i < numBytes; i++) {
      bytes[pos + i] = (byte) (value >>> (8 * i));
    }
  }

  /** Returns the smallest number of bytes (1 to 4) that can hold the unsigned {@code maxValue}. */
  static int sizeOf(long maxValue) {
    if (maxValue <= 0xFFL) {
      return 1;
    } else if (maxValue <= 0xFFFFL) {
      return 2;
    } else if (maxValue <= 0xFFFFFFL) {
      return 3;
    }
    return 4;
  }

  static byte primitiveHeader(int typeId) {
    return (byte) (typeId << 2);
  }

  static byte shortStringHeader(int length) {
    return (byte) ((length << 2) | BASIC_TYPE_SHORT_STRING);
  }

  static byte objectHeader(boolean isLarge, int fieldIdSize, int offsetSize) {
    return (byte)
        (BASIC_TYPE_OBJECT
            | ((offsetSize - 1) << 2)
            | ((fieldIdSize - 1) << 4)
            | (isLarge ? (1 << 6) : 0));
  }

  static byte arrayHeader(boolean isLarge, int offsetSize) {
    return (byte) (BASIC_TYPE_ARRAY | ((offsetSize - 1) << 2) | (isLarge ? (1 << 4) : 0));
  }

  /**
   * Compares {@code a[aStart, aStart + aLength)} with all of {@code b} using unsigned lexicographic
   * byte order, which is the field-name order the spec requires for UTF-8 keys.
   */
  static int compareUtf8(byte[] a, int aStart, int aLength, byte[] b) {
    int length = Math.min(aLength, b.length);
    for (int i = 0; i < length; i++) {
      int cmp = (a[aStart + i] & 0xFF) - (b[i] & 0xFF);
      if (cmp != 0) {
        return cmp;
      }
    }
    return aLength - b.length;
  }

  static int compareUtf8(byte[] a, byte[] b) {
    return compareUtf8(a, 0, a.length, b);
  }
}
