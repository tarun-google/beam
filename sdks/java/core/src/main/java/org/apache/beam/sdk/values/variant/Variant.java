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

import static org.apache.beam.sdk.values.variant.VariantUtil.BASIC_TYPE_ARRAY;
import static org.apache.beam.sdk.values.variant.VariantUtil.BASIC_TYPE_MASK;
import static org.apache.beam.sdk.values.variant.VariantUtil.BASIC_TYPE_OBJECT;
import static org.apache.beam.sdk.values.variant.VariantUtil.BASIC_TYPE_PRIMITIVE;
import static org.apache.beam.sdk.values.variant.VariantUtil.BASIC_TYPE_SHORT_STRING;
import static org.apache.beam.sdk.values.variant.VariantUtil.MAX_DECIMAL_PRECISION;
import static org.apache.beam.sdk.values.variant.VariantUtil.MAX_SIZE;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_BINARY;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_DATE;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_DECIMAL16;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_DECIMAL4;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_DECIMAL8;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_DOUBLE;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_FALSE;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_FLOAT;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_INT16;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_INT32;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_INT64;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_INT8;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_NULL;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_STRING;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_TIME;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_TIMESTAMP;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_TIMESTAMP_NANOS;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_TIMESTAMP_NTZ;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_TIMESTAMP_NTZ_NANOS;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_TRUE;
import static org.apache.beam.sdk.values.variant.VariantUtil.TYPE_UUID;
import static org.apache.beam.sdk.values.variant.VariantUtil.checkBounds;
import static org.apache.beam.sdk.values.variant.VariantUtil.checkNestingDepth;
import static org.apache.beam.sdk.values.variant.VariantUtil.malformed;
import static org.apache.beam.sdk.values.variant.VariantUtil.readSigned;
import static org.apache.beam.sdk.values.variant.VariantUtil.readUnsigned;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An immutable semi-structured value, like a JSON document but with a richer set of types.
 *
 * <p>A {@code Variant} is one of:
 *
 * <ul>
 *   <li>a primitive: null, boolean, integer, float, double, decimal, string, binary, date, time,
 *       timestamp (with or without time zone, micro- or nanosecond precision) or UUID;
 *   <li>an array: an ordered list of Variant values, which may have different types;
 *   <li>an object: a set of string keys, each mapped to a Variant value.
 * </ul>
 *
 * <p>It is stored exactly as defined by the Apache Parquet <a
 * href="https://github.com/apache/parquet-format/blob/master/VariantEncoding.md">Variant binary
 * encoding</a> (version 1): a {@code metadata} buffer holding the field-name dictionary and a
 * {@code value} buffer holding the data. Because the bytes follow the spec, the same value can be
 * handed to Apache Iceberg, Apache Spark, Apache Flink, Parquet or Arrow without conversion.
 *
 * <p>Reads are lazy and work directly on the bytes. Nested values share the parent's buffers,
 * object fields are found by binary search over the name-sorted field IDs, and nothing is decoded
 * until it is asked for.
 *
 * <p>Equality is logical rather than byte-wise: values compare equal if they have the same
 * structure and their primitives are equal within the same equivalence class. For example, the
 * integer {@code 1} equals the decimal {@code 1.00}, and object field order never matters.
 *
 * <p>Creating a Variant from bytes validates the metadata and the top-level value header. Nested
 * values are bounds-checked when they are read, and malformed input raises {@link
 * IllegalArgumentException}.
 */
public final class Variant implements Serializable {
  private static final long serialVersionUID = 1L;

  /** The type of a {@link Variant} value. */
  public enum Type {
    OBJECT(Kind.OBJECT),
    ARRAY(Kind.ARRAY),
    NULL(Kind.NULL),
    BOOLEAN(Kind.BOOLEAN),
    INT8(Kind.EXACT_NUMERIC),
    INT16(Kind.EXACT_NUMERIC),
    INT32(Kind.EXACT_NUMERIC),
    INT64(Kind.EXACT_NUMERIC),
    DECIMAL4(Kind.EXACT_NUMERIC),
    DECIMAL8(Kind.EXACT_NUMERIC),
    DECIMAL16(Kind.EXACT_NUMERIC),
    FLOAT(Kind.FLOAT),
    DOUBLE(Kind.DOUBLE),
    STRING(Kind.STRING),
    BINARY(Kind.BINARY),
    DATE(Kind.DATE),
    /** Time of day without time zone, microsecond precision. */
    TIME(Kind.TIME),
    /** Timestamp with time zone (an instant), microsecond precision. */
    TIMESTAMP(Kind.TIMESTAMP),
    /** Timestamp with time zone (an instant), nanosecond precision. */
    TIMESTAMP_NANOS(Kind.TIMESTAMP),
    /** Timestamp without time zone (a local date-time), microsecond precision. */
    TIMESTAMP_NTZ(Kind.TIMESTAMP_NTZ),
    /** Timestamp without time zone (a local date-time), nanosecond precision. */
    TIMESTAMP_NTZ_NANOS(Kind.TIMESTAMP_NTZ),
    UUID(Kind.UUID);

    /** The spec's equivalence class: types in the same class compare by value. */
    private final Kind kind;

    Type(Kind kind) {
      this.kind = kind;
    }
  }

  private enum Kind {
    OBJECT,
    ARRAY,
    NULL,
    BOOLEAN,
    EXACT_NUMERIC,
    FLOAT,
    DOUBLE,
    STRING,
    BINARY,
    DATE,
    TIME,
    TIMESTAMP,
    TIMESTAMP_NTZ,
    UUID
  }

  private static final Variant NULL_VARIANT = Variant.of(null);

  private final byte[] metadataBytes;
  final byte[] value;
  final int pos;
  private transient @Nullable VariantMetadata metadata;

  private Variant(byte[] metadataBytes, @Nullable VariantMetadata metadata, byte[] value, int pos) {
    this.metadataBytes = metadataBytes;
    this.metadata = metadata;
    this.value = value;
    this.pos = pos;
  }

  // ---------------------------------------------------------------------------------------------
  // Factory methods

  /**
   * Creates a Variant from its spec-encoded {@code metadata} and {@code value} buffers, for example
   * bytes read from a Parquet or Iceberg {@code variant} column. The arrays are copied.
   *
   * @throws IllegalArgumentException if the metadata or the top-level value is malformed
   */
  public static Variant fromBytes(byte[] metadata, byte[] value) {
    return wrap(metadata.clone(), value.clone());
  }

  /** Like {@link #fromBytes} but takes ownership of the arrays instead of copying them. */
  static Variant wrap(byte[] metadata, byte[] value) {
    if (value.length == 0) {
      throw malformed("value is empty");
    }
    if (value.length > MAX_SIZE) {
      throw malformed("value is larger than " + MAX_SIZE + " bytes");
    }
    Variant variant = new Variant(metadata, new VariantMetadata(metadata), value, 0);
    variant.valueSize(); // validates the top-level header and size
    return variant;
  }

  /**
   * Parses a JSON document into a Variant, like Spark's {@code parse_json}.
   *
   * <p>Integers use the smallest integer type that fits. Numbers with a fractional part and no
   * exponent are stored as exact decimals when they fit in 38 digits; other numbers become doubles.
   * Strings stay strings: no timestamps are inferred. Duplicate keys are rejected.
   *
   * @throws IllegalArgumentException if {@code json} is not valid JSON
   */
  public static Variant parseJson(String json) {
    return VariantJson.parse(json);
  }

  /**
   * Like {@link #parseJson} but returns {@code null} instead of throwing when the input is not
   * valid JSON, like Spark's {@code try_parse_json}.
   */
  public static @Nullable Variant tryParseJson(@Nullable String json) {
    if (json == null) {
      return null;
    }
    try {
      return VariantJson.parse(json);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /** Returns a Variant holding the Variant null value. This is not the same as a Java null. */
  public static Variant ofNull() {
    return NULL_VARIANT;
  }

  /**
   * Converts a plain Java value into a Variant.
   *
   * <p>Supported types: {@code null}, {@link Boolean}, {@link Byte}, {@link Short}, {@link
   * Integer}, {@link Long}, {@link java.math.BigInteger}, {@link BigDecimal}, {@link Float}, {@link
   * Double}, {@link CharSequence}, {@code byte[]}, {@link java.nio.ByteBuffer}, {@link LocalDate},
   * {@link LocalTime}, {@link LocalDateTime} (timestamp without time zone), {@link Instant} and
   * Joda {@code ReadableInstant} (timestamp with time zone), {@link UUID}, {@link java.util.Map}
   * with string keys (object), {@link Iterable} or {@code Object[]} (array), and {@code Variant}.
   * Collections are converted recursively.
   *
   * @throws IllegalArgumentException if the value or anything nested in it is not supported
   */
  public static Variant of(@Nullable Object value) {
    VariantEncoder encoder = new VariantEncoder();
    encoder.appendJava(value, 0);
    return encoder.build();
  }

  // ---------------------------------------------------------------------------------------------
  // Type and bytes

  /** Returns the type of this value. */
  public Type getType() {
    int header = header();
    switch (header & BASIC_TYPE_MASK) {
      case BASIC_TYPE_SHORT_STRING:
        return Type.STRING;
      case BASIC_TYPE_OBJECT:
        return Type.OBJECT;
      case BASIC_TYPE_ARRAY:
        return Type.ARRAY;
      default:
        return primitiveType(header >> 2);
    }
  }

  /** Returns true if this is the Variant null value. */
  public boolean isNull() {
    return primitiveTypeId() == TYPE_NULL;
  }

  /**
   * Returns a copy of the spec-encoded metadata buffer. For a nested value this is the metadata of
   * the whole document, which the spec allows.
   */
  public byte[] getMetadata() {
    return metadataBytes.clone();
  }

  /** Returns a copy of the spec-encoded bytes of this value. */
  public byte[] getValue() {
    return Arrays.copyOfRange(value, pos, pos + valueSize());
  }

  // ---------------------------------------------------------------------------------------------
  // Primitive accessors. Each throws IllegalStateException if the type does not match.

  /** Returns this value as a boolean. */
  public boolean getBoolean() {
    int typeId = primitiveTypeId();
    if (typeId == TYPE_TRUE) {
      return true;
    } else if (typeId == TYPE_FALSE) {
      return false;
    }
    throw typeMismatch("BOOLEAN");
  }

  /** Returns an integer value of any width (INT8 to INT64) as a long. */
  public long getLong() {
    switch (primitiveTypeId()) {
      case TYPE_INT8:
        return readSigned(value, pos + 1L, 1);
      case TYPE_INT16:
        return readSigned(value, pos + 1L, 2);
      case TYPE_INT32:
        return readSigned(value, pos + 1L, 4);
      case TYPE_INT64:
        return readSigned(value, pos + 1L, 8);
      default:
        throw typeMismatch("LONG");
    }
  }

  /**
   * Returns an integer value as an int.
   *
   * @throws ArithmeticException if the value does not fit in an int
   */
  public int getInt() {
    long result = getLong();
    if (result != (int) result) {
      throw new ArithmeticException("Variant integer " + result + " does not fit in an int");
    }
    return (int) result;
  }

  /** Returns a FLOAT value. */
  public float getFloat() {
    if (primitiveTypeId() != TYPE_FLOAT) {
      throw typeMismatch("FLOAT");
    }
    return Float.intBitsToFloat((int) readSigned(value, pos + 1L, 4));
  }

  /** Returns any numeric value (integer, decimal, float or double) as a double. */
  public double getDouble() {
    switch (primitiveTypeId()) {
      case TYPE_DOUBLE:
        return Double.longBitsToDouble(readSigned(value, pos + 1L, 8));
      case TYPE_FLOAT:
        return getFloat();
      case TYPE_INT8:
      case TYPE_INT16:
      case TYPE_INT32:
      case TYPE_INT64:
        return getLong();
      case TYPE_DECIMAL4:
      case TYPE_DECIMAL8:
      case TYPE_DECIMAL16:
        return getDecimal().doubleValue();
      default:
        throw typeMismatch("DOUBLE");
    }
  }

  /** Returns an exact numeric value (integer or decimal) as a BigDecimal. */
  public BigDecimal getDecimal() {
    switch (primitiveTypeId()) {
      case TYPE_INT8:
      case TYPE_INT16:
      case TYPE_INT32:
      case TYPE_INT64:
        return BigDecimal.valueOf(getLong());
      case TYPE_DECIMAL4:
        return readDecimal(4);
      case TYPE_DECIMAL8:
        return readDecimal(8);
      case TYPE_DECIMAL16:
        return readDecimal(16);
      default:
        throw typeMismatch("DECIMAL");
    }
  }

  /** Returns a STRING value. */
  public String getString() {
    int header = header();
    int basicType = header & BASIC_TYPE_MASK;
    if (basicType == BASIC_TYPE_SHORT_STRING) {
      int length = header >> 2;
      checkBounds(value, pos + 1L, length, "short string");
      return new String(value, pos + 1, length, StandardCharsets.UTF_8);
    }
    if (basicType == BASIC_TYPE_PRIMITIVE && (header >> 2) == TYPE_STRING) {
      long length = readUnsigned(value, pos + 1L, 4);
      checkBounds(value, pos + 5L, length, "string");
      return new String(value, pos + 5, (int) length, StandardCharsets.UTF_8);
    }
    throw typeMismatch("STRING");
  }

  /** Returns a copy of a BINARY value. */
  public byte[] getBinary() {
    if (primitiveTypeId() != TYPE_BINARY) {
      throw typeMismatch("BINARY");
    }
    long length = readUnsigned(value, pos + 1L, 4);
    checkBounds(value, pos + 5L, length, "binary");
    return Arrays.copyOfRange(value, pos + 5, pos + 5 + (int) length);
  }

  /** Returns a DATE value. */
  public LocalDate getDate() {
    if (primitiveTypeId() != TYPE_DATE) {
      throw typeMismatch("DATE");
    }
    return LocalDate.ofEpochDay(readSigned(value, pos + 1L, 4));
  }

  /** Returns a TIME value (time of day, no time zone). */
  public LocalTime getTime() {
    if (primitiveTypeId() != TYPE_TIME) {
      throw typeMismatch("TIME");
    }
    long micros = readSigned(value, pos + 1L, 8);
    if (micros < 0 || micros >= 86_400_000_000L) {
      throw malformed("time of day " + micros + " is out of range");
    }
    return LocalTime.ofNanoOfDay(micros * 1000);
  }

  /** Returns a TIMESTAMP or TIMESTAMP_NANOS value (with time zone) as an instant. */
  public Instant getTimestamp() {
    switch (primitiveTypeId()) {
      case TYPE_TIMESTAMP:
        {
          long micros = readSigned(value, pos + 1L, 8);
          return Instant.ofEpochSecond(
              Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000L);
        }
      case TYPE_TIMESTAMP_NANOS:
        {
          long nanos = readSigned(value, pos + 1L, 8);
          return Instant.ofEpochSecond(
              Math.floorDiv(nanos, 1_000_000_000L), Math.floorMod(nanos, 1_000_000_000L));
        }
      default:
        throw typeMismatch("TIMESTAMP");
    }
  }

  /** Returns a TIMESTAMP_NTZ or TIMESTAMP_NTZ_NANOS value (without time zone). */
  public LocalDateTime getTimestampNtz() {
    long seconds;
    long nanos;
    switch (primitiveTypeId()) {
      case TYPE_TIMESTAMP_NTZ:
        {
          long micros = readSigned(value, pos + 1L, 8);
          seconds = Math.floorDiv(micros, 1_000_000L);
          nanos = Math.floorMod(micros, 1_000_000L) * 1000L;
          break;
        }
      case TYPE_TIMESTAMP_NTZ_NANOS:
        {
          long total = readSigned(value, pos + 1L, 8);
          seconds = Math.floorDiv(total, 1_000_000_000L);
          nanos = Math.floorMod(total, 1_000_000_000L);
          break;
        }
      default:
        throw typeMismatch("TIMESTAMP_NTZ");
    }
    return LocalDateTime.ofEpochSecond(seconds, (int) nanos, ZoneOffset.UTC);
  }

  /** Returns a UUID value. */
  public UUID getUuid() {
    if (primitiveTypeId() != TYPE_UUID) {
      throw typeMismatch("UUID");
    }
    checkBounds(value, pos + 1L, 16, "uuid");
    return new UUID(readBigEndianLong(pos + 1), readBigEndianLong(pos + 9));
  }

  // ---------------------------------------------------------------------------------------------
  // Objects and arrays

  /** Returns the number of fields of an OBJECT value. */
  public int numFields() {
    checkKind(BASIC_TYPE_OBJECT, "OBJECT");
    return container().numElements;
  }

  /** Returns the field names of an OBJECT value, in the spec's sorted order. */
  public List<String> getFieldNames() {
    checkKind(BASIC_TYPE_OBJECT, "OBJECT");
    Container object = container();
    List<String> names = new ArrayList<>(object.numElements);
    for (int i = 0; i < object.numElements; i++) {
      names.add(metadata().get(fieldIdAt(object, i)));
    }
    return Collections.unmodifiableList(names);
  }

  /**
   * Returns the value of the field named {@code name} in an OBJECT value, or {@code null} if the
   * object has no such field. The lookup is a binary search over the sorted field IDs.
   */
  public @Nullable Variant getField(String name) {
    checkKind(BASIC_TYPE_OBJECT, "OBJECT");
    Container object = container();
    VariantMetadata dictionary = metadata();
    byte[] key = name.getBytes(StandardCharsets.UTF_8);
    int low = 0;
    int high = object.numElements - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      int cmp = dictionary.compare(fieldIdAt(object, mid), key);
      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return childAt(object, mid);
      }
    }
    // UTF-8 byte order and Java's UTF-16 order differ only for characters at or above U+D800.
    // Some older writers sorted with String.compareTo, so scan linearly to stay compatible.
    if (hasCharAtOrAboveSurrogates(name)) {
      for (int i = 0; i < object.numElements; i++) {
        if (dictionary.compare(fieldIdAt(object, i), key) == 0) {
          return childAt(object, i);
        }
      }
    }
    return null;
  }

  /** Returns the number of elements of an ARRAY value. */
  public int numElements() {
    checkKind(BASIC_TYPE_ARRAY, "ARRAY");
    return container().numElements;
  }

  /**
   * Returns the element at {@code index} of an ARRAY value.
   *
   * @throws IndexOutOfBoundsException if {@code index} is out of range
   */
  public Variant getElement(int index) {
    checkKind(BASIC_TYPE_ARRAY, "ARRAY");
    Container array = container();
    if (index < 0 || index >= array.numElements) {
      throw new IndexOutOfBoundsException(
          "Index " + index + " is out of range for a variant array of size " + array.numElements);
    }
    return childAt(array, index);
  }

  /**
   * Returns the value at a JSON path, or {@code null} if the path does not exist.
   *
   * <p>The path starts with {@code $} (this value) followed by any number of steps: {@code .name}
   * or {@code ['name']} for an object field, and {@code [n]} for an array element. For example
   * {@code $.user.addresses[0]['zip code']}. A step that does not match, such as a missing field,
   * an index past the end, or a field step on an array, returns {@code null}.
   *
   * @throws IllegalArgumentException if {@code path} is not a valid path
   */
  public @Nullable Variant get(String path) {
    Variant current = this;
    for (Object step : VariantPath.parse(path)) {
      if (step instanceof String) {
        if (current.getType() != Type.OBJECT) {
          return null;
        }
        Variant field = current.getField((String) step);
        if (field == null) {
          return null;
        }
        current = field;
      } else {
        int index = (Integer) step;
        if (current.getType() != Type.ARRAY || index >= current.numElements()) {
          return null;
        }
        current = current.getElement(index);
      }
    }
    return current;
  }

  // ---------------------------------------------------------------------------------------------
  // JSON, equality and toString

  /**
   * Returns this value as compact JSON, like Spark's {@code to_json}. Object keys appear in sorted
   * order. Types that JSON lacks are written as strings: dates, times and timestamps in ISO-8601,
   * binary in base64, and UUIDs in their canonical form.
   */
  public String toJson() {
    return VariantJson.toJson(this);
  }

  @Override
  public String toString() {
    try {
      return toJson();
    } catch (IllegalArgumentException | UnsupportedOperationException e) {
      // Never throw from toString: it is called by loggers, debuggers and assertion messages.
      return "Variant(" + e.getMessage() + ")";
    }
  }

  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Variant)) {
      return false;
    }
    return logicallyEqual(this, (Variant) other, 0);
  }

  @Override
  public int hashCode() {
    return logicalHash(this, 0);
  }

  private static boolean logicallyEqual(Variant a, Variant b, int depth) {
    Type type = a.getType();
    Type otherType = b.getType();
    if (type.kind != otherType.kind) {
      return false;
    }
    switch (type.kind) {
      case NULL:
        return true;
      case BOOLEAN:
        return a.getBoolean() == b.getBoolean();
      case EXACT_NUMERIC:
        return a.getDecimal().compareTo(b.getDecimal()) == 0;
      case FLOAT:
        return Float.floatToIntBits(a.getFloat()) == Float.floatToIntBits(b.getFloat());
      case DOUBLE:
        return Double.doubleToLongBits(a.getDouble()) == Double.doubleToLongBits(b.getDouble());
      case STRING:
        return a.getString().equals(b.getString());
      case BINARY:
        return Arrays.equals(a.getBinary(), b.getBinary());
      case DATE:
        return a.getDate().equals(b.getDate());
      case TIME:
        return a.getTime().equals(b.getTime());
      case TIMESTAMP:
        return a.getTimestamp().equals(b.getTimestamp());
      case TIMESTAMP_NTZ:
        return a.getTimestampNtz().equals(b.getTimestampNtz());
      case UUID:
        return a.getUuid().equals(b.getUuid());
      case ARRAY:
        {
          checkNestingDepth(depth);
          int size = a.numElements();
          if (size != b.numElements()) {
            return false;
          }
          for (int i = 0; i < size; i++) {
            if (!logicallyEqual(a.getElement(i), b.getElement(i), depth + 1)) {
              return false;
            }
          }
          return true;
        }
      case OBJECT:
        {
          checkNestingDepth(depth);
          int size = a.numFields();
          if (size != b.numFields()) {
            return false;
          }
          for (int i = 0; i < size; i++) {
            Variant otherField = b.getField(a.fieldNameAt(i));
            if (otherField == null || !logicallyEqual(a.fieldValueAt(i), otherField, depth + 1)) {
              return false;
            }
          }
          return true;
        }
      default:
        throw new IllegalStateException("Unexpected variant kind: " + type.kind);
    }
  }

  private static int logicalHash(Variant v, int depth) {
    Kind kind = v.getType().kind;
    int hash;
    switch (kind) {
      case NULL:
        hash = 0;
        break;
      case BOOLEAN:
        hash = Boolean.hashCode(v.getBoolean());
        break;
      case EXACT_NUMERIC:
        // Strip trailing zeros so that, for example, 1, 1.0 and 1.00 hash the same.
        hash = v.getDecimal().stripTrailingZeros().hashCode();
        break;
      case FLOAT:
        hash = Float.hashCode(v.getFloat());
        break;
      case DOUBLE:
        hash = Double.hashCode(v.getDouble());
        break;
      case STRING:
        hash = v.getString().hashCode();
        break;
      case BINARY:
        hash = Arrays.hashCode(v.getBinary());
        break;
      case DATE:
        hash = v.getDate().hashCode();
        break;
      case TIME:
        hash = v.getTime().hashCode();
        break;
      case TIMESTAMP:
        hash = v.getTimestamp().hashCode();
        break;
      case TIMESTAMP_NTZ:
        hash = v.getTimestampNtz().hashCode();
        break;
      case UUID:
        hash = v.getUuid().hashCode();
        break;
      case ARRAY:
        checkNestingDepth(depth);
        hash = 1;
        for (int i = 0; i < v.numElements(); i++) {
          hash = 31 * hash + logicalHash(v.getElement(i), depth + 1);
        }
        break;
      case OBJECT:
        checkNestingDepth(depth);
        // A sum is independent of field order.
        hash = 0;
        for (int i = 0; i < v.numFields(); i++) {
          hash += Objects.hash(v.fieldNameAt(i), logicalHash(v.fieldValueAt(i), depth + 1));
        }
        break;
      default:
        throw new IllegalStateException("Unexpected variant kind: " + kind);
    }
    return 31 * kind.ordinal() + hash;
  }

  // ---------------------------------------------------------------------------------------------
  // Package-private helpers used by the encoder and the JSON writer

  /** Returns the size in bytes of this value, validating that it fits in the buffer. */
  int valueSize() {
    int header = header();
    int basicType = header & BASIC_TYPE_MASK;
    long size;
    if (basicType == BASIC_TYPE_SHORT_STRING) {
      size = 1L + (header >> 2);
    } else if (basicType == BASIC_TYPE_OBJECT || basicType == BASIC_TYPE_ARRAY) {
      Container container = container();
      size = container.dataStart - pos + container.dataSize;
    } else {
      int typeId = header >> 2;
      switch (typeId) {
        case TYPE_NULL:
        case TYPE_TRUE:
        case TYPE_FALSE:
          size = 1;
          break;
        case TYPE_INT8:
          size = 2;
          break;
        case TYPE_INT16:
          size = 3;
          break;
        case TYPE_INT32:
        case TYPE_DATE:
        case TYPE_FLOAT:
          size = 5;
          break;
        case TYPE_DECIMAL4:
          size = 6;
          break;
        case TYPE_INT64:
        case TYPE_DOUBLE:
        case TYPE_TIMESTAMP:
        case TYPE_TIMESTAMP_NTZ:
        case TYPE_TIME:
        case TYPE_TIMESTAMP_NANOS:
        case TYPE_TIMESTAMP_NTZ_NANOS:
          size = 9;
          break;
        case TYPE_DECIMAL8:
          size = 10;
          break;
        case TYPE_UUID:
          size = 17;
          break;
        case TYPE_DECIMAL16:
          size = 18;
          break;
        case TYPE_BINARY:
        case TYPE_STRING:
          size = 5 + readUnsigned(value, pos + 1L, 4);
          break;
        default:
          throw unsupportedPrimitive(typeId);
      }
    }
    checkBounds(value, pos, size, "variant value");
    return (int) size;
  }

  /** Returns the name of field {@code index} (in sorted order) of an OBJECT value. */
  String fieldNameAt(int index) {
    return metadata().get(fieldIdAt(container(), index));
  }

  /** Returns the value of field {@code index} (in sorted order) of an OBJECT value. */
  Variant fieldValueAt(int index) {
    return childAt(container(), index);
  }

  // ---------------------------------------------------------------------------------------------
  // Private decoding helpers

  /** The parsed header of an object or array value. */
  private static final class Container {
    final int numElements;
    final int fieldIdSize;
    final int offsetSize;
    final int idsStart;
    final int offsetsStart;
    final int dataStart;
    final long dataSize;

    Container(
        int numElements,
        int fieldIdSize,
        int offsetSize,
        int idsStart,
        int offsetsStart,
        int dataStart,
        long dataSize) {
      this.numElements = numElements;
      this.fieldIdSize = fieldIdSize;
      this.offsetSize = offsetSize;
      this.idsStart = idsStart;
      this.offsetsStart = offsetsStart;
      this.dataStart = dataStart;
      this.dataSize = dataSize;
    }
  }

  private VariantMetadata metadata() {
    VariantMetadata result = metadata;
    if (result == null) {
      result = new VariantMetadata(metadataBytes);
      metadata = result;
    }
    return result;
  }

  private int header() {
    checkBounds(value, pos, 1, "value header");
    return value[pos] & 0xFF;
  }

  /** Returns the primitive type ID, or -1 if this is not a primitive (or short string) value. */
  private int primitiveTypeId() {
    int header = header();
    return (header & BASIC_TYPE_MASK) == BASIC_TYPE_PRIMITIVE ? header >> 2 : -1;
  }

  private Container container() {
    int header = header();
    boolean isObject = (header & BASIC_TYPE_MASK) == BASIC_TYPE_OBJECT;
    int offsetSize = ((header >> 2) & 0x3) + 1;
    int fieldIdSize = isObject ? ((header >> 4) & 0x3) + 1 : 0;
    boolean isLarge = ((header >> (isObject ? 6 : 4)) & 0x1) != 0;
    int sizeBytes = isLarge ? 4 : 1;
    long numElements = readUnsigned(value, pos + 1L, sizeBytes);
    long idsStart = pos + 1L + sizeBytes;
    long offsetsStart = idsStart + numElements * fieldIdSize;
    long dataStart = offsetsStart + (numElements + 1) * offsetSize;
    String what = isObject ? "object" : "array";
    checkBounds(value, pos, dataStart - pos, what + " header");
    long dataSize = readUnsigned(value, offsetsStart + numElements * offsetSize, offsetSize);
    checkBounds(value, dataStart, dataSize, what + " data");
    return new Container(
        (int) numElements,
        fieldIdSize,
        offsetSize,
        (int) idsStart,
        (int) offsetsStart,
        (int) dataStart,
        dataSize);
  }

  private int fieldIdAt(Container object, int index) {
    long id =
        readUnsigned(
            value, object.idsStart + (long) index * object.fieldIdSize, object.fieldIdSize);
    if (id >= metadata().size()) {
      throw malformed("field id " + id + " is outside the metadata dictionary");
    }
    return (int) id;
  }

  private Variant childAt(Container container, int index) {
    long offset =
        readUnsigned(
            value,
            container.offsetsStart + (long) index * container.offsetSize,
            container.offsetSize);
    if (offset >= container.dataSize) {
      throw malformed("child offset " + offset + " is outside its container");
    }
    return new Variant(metadataBytes, metadata(), value, (int) (container.dataStart + offset));
  }

  private BigDecimal readDecimal(int unscaledBytes) {
    checkBounds(value, pos + 1L, 1L + unscaledBytes, "decimal");
    int scale = value[pos + 1] & 0xFF;
    if (scale > MAX_DECIMAL_PRECISION) {
      throw malformed("decimal scale " + scale + " is greater than " + MAX_DECIMAL_PRECISION);
    }
    BigInteger unscaled;
    if (unscaledBytes <= 8) {
      unscaled = BigInteger.valueOf(readSigned(value, pos + 2L, unscaledBytes));
    } else {
      byte[] bigEndian = new byte[unscaledBytes];
      for (int i = 0; i < unscaledBytes; i++) {
        bigEndian[i] = value[pos + 2 + unscaledBytes - 1 - i];
      }
      unscaled = new BigInteger(bigEndian);
    }
    return new BigDecimal(unscaled, scale);
  }

  private long readBigEndianLong(int start) {
    long result = 0;
    for (int i = 0; i < 8; i++) {
      result = (result << 8) | (value[start + i] & 0xFFL);
    }
    return result;
  }

  private void checkKind(int basicType, String typeName) {
    if ((header() & BASIC_TYPE_MASK) != basicType) {
      throw typeMismatch(typeName);
    }
  }

  private IllegalStateException typeMismatch(String requested) {
    return new IllegalStateException(
        "Cannot read a " + getType() + " variant value as " + requested);
  }

  private static Type primitiveType(int typeId) {
    switch (typeId) {
      case TYPE_NULL:
        return Type.NULL;
      case TYPE_TRUE:
      case TYPE_FALSE:
        return Type.BOOLEAN;
      case TYPE_INT8:
        return Type.INT8;
      case TYPE_INT16:
        return Type.INT16;
      case TYPE_INT32:
        return Type.INT32;
      case TYPE_INT64:
        return Type.INT64;
      case TYPE_DOUBLE:
        return Type.DOUBLE;
      case TYPE_DECIMAL4:
        return Type.DECIMAL4;
      case TYPE_DECIMAL8:
        return Type.DECIMAL8;
      case TYPE_DECIMAL16:
        return Type.DECIMAL16;
      case TYPE_DATE:
        return Type.DATE;
      case TYPE_TIMESTAMP:
        return Type.TIMESTAMP;
      case TYPE_TIMESTAMP_NTZ:
        return Type.TIMESTAMP_NTZ;
      case TYPE_FLOAT:
        return Type.FLOAT;
      case TYPE_BINARY:
        return Type.BINARY;
      case TYPE_STRING:
        return Type.STRING;
      case TYPE_TIME:
        return Type.TIME;
      case TYPE_TIMESTAMP_NANOS:
        return Type.TIMESTAMP_NANOS;
      case TYPE_TIMESTAMP_NTZ_NANOS:
        return Type.TIMESTAMP_NTZ_NANOS;
      case TYPE_UUID:
        return Type.UUID;
      default:
        throw unsupportedPrimitive(typeId);
    }
  }

  private static UnsupportedOperationException unsupportedPrimitive(int typeId) {
    return new UnsupportedOperationException("Unsupported variant primitive type id: " + typeId);
  }

  private static boolean hasCharAtOrAboveSurrogates(String name) {
    for (int i = 0; i < name.length(); i++) {
      if (name.charAt(i) >= Character.MIN_SURROGATE) {
        return true;
      }
    }
    return false;
  }
}
