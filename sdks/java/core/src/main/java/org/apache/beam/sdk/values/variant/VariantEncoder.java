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

import static org.apache.beam.sdk.values.variant.VariantUtil.MAX_DECIMAL4_PRECISION;
import static org.apache.beam.sdk.values.variant.VariantUtil.MAX_DECIMAL8_PRECISION;
import static org.apache.beam.sdk.values.variant.VariantUtil.MAX_DECIMAL_PRECISION;
import static org.apache.beam.sdk.values.variant.VariantUtil.MAX_SHORT_STRING_BYTES;
import static org.apache.beam.sdk.values.variant.VariantUtil.MAX_SIZE;
import static org.apache.beam.sdk.values.variant.VariantUtil.SORTED_STRINGS_BIT;
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
import static org.apache.beam.sdk.values.variant.VariantUtil.VERSION;
import static org.apache.beam.sdk.values.variant.VariantUtil.arrayHeader;
import static org.apache.beam.sdk.values.variant.VariantUtil.checkNestingDepth;
import static org.apache.beam.sdk.values.variant.VariantUtil.compareUtf8;
import static org.apache.beam.sdk.values.variant.VariantUtil.objectHeader;
import static org.apache.beam.sdk.values.variant.VariantUtil.primitiveHeader;
import static org.apache.beam.sdk.values.variant.VariantUtil.shortStringHeader;
import static org.apache.beam.sdk.values.variant.VariantUtil.sizeOf;
import static org.apache.beam.sdk.values.variant.VariantUtil.writeLittleEndian;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.ReadableInstant;

/**
 * Builds one Variant value: a {@code value} buffer plus the {@code metadata} dictionary of field
 * names it uses.
 *
 * <p>Values are appended depth-first. For an object, the caller records where each field's value
 * starts, appends the values, and then calls {@link #finishObject}. That method sorts the fields by
 * name, as the spec requires, and inserts the object header in front of the values that were
 * already written. Arrays work the same way through {@link #finishArray}.
 */
final class VariantEncoder {

  /** One field of an object that is being built. */
  static final class FieldEntry {
    final int id;
    final int offset;

    FieldEntry(int id, int offset) {
      this.id = id;
      this.offset = offset;
    }
  }

  private byte[] buffer = new byte[64];
  private int position = 0;
  private final Map<String, Integer> dictionary = new HashMap<>();
  private final List<byte[]> dictionaryKeys = new ArrayList<>();
  private long dictionaryBytes = 0;

  /** Returns the number of value bytes written so far. */
  int position() {
    return position;
  }

  /** Returns the dictionary ID for {@code key}, adding it if it is new. */
  int addKey(String key) {
    Integer existing = dictionary.get(key);
    if (existing != null) {
      return existing;
    }
    byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
    dictionaryBytes += keyBytes.length;
    if (dictionaryBytes > MAX_SIZE) {
      throw new IllegalArgumentException(
          "Variant metadata would exceed the maximum size of " + MAX_SIZE + " bytes");
    }
    int id = dictionaryKeys.size();
    dictionary.put(key, id);
    dictionaryKeys.add(keyBytes);
    return id;
  }

  void appendNull() {
    writePrimitiveHeader(TYPE_NULL, 0);
  }

  void appendBoolean(boolean value) {
    writePrimitiveHeader(value ? TYPE_TRUE : TYPE_FALSE, 0);
  }

  /** Appends an integer using the smallest integer type that can hold it, like Spark does. */
  void appendLong(long value) {
    if (value == (byte) value) {
      writePrimitive(TYPE_INT8, value, 1);
    } else if (value == (short) value) {
      writePrimitive(TYPE_INT16, value, 2);
    } else if (value == (int) value) {
      writePrimitive(TYPE_INT32, value, 4);
    } else {
      writePrimitive(TYPE_INT64, value, 8);
    }
  }

  void appendDouble(double value) {
    writePrimitive(TYPE_DOUBLE, Double.doubleToRawLongBits(value), 8);
  }

  void appendFloat(float value) {
    writePrimitive(TYPE_FLOAT, Float.floatToRawIntBits(value), 4);
  }

  /** Appends a decimal using the smallest decimal type that can hold its precision. */
  void appendDecimal(BigDecimal decimal) {
    // A negative scale means the value is an integer, so widening the scale to 0 is exact.
    BigDecimal normalized = decimal.scale() < 0 ? decimal.setScale(0) : decimal;
    int precision = normalized.precision();
    int scale = normalized.scale();
    if (precision > MAX_DECIMAL_PRECISION || scale > MAX_DECIMAL_PRECISION) {
      throw new IllegalArgumentException(
          "Decimal "
              + decimal
              + " exceeds the maximum Variant precision and scale of "
              + MAX_DECIMAL_PRECISION);
    }
    BigInteger unscaled = normalized.unscaledValue();
    if (precision <= MAX_DECIMAL4_PRECISION) {
      writePrimitiveHeader(TYPE_DECIMAL4, 5);
      buffer[position++] = (byte) scale;
      writeLittleEndian(buffer, position, unscaled.intValue(), 4);
      position += 4;
    } else if (precision <= MAX_DECIMAL8_PRECISION) {
      writePrimitiveHeader(TYPE_DECIMAL8, 9);
      buffer[position++] = (byte) scale;
      writeLittleEndian(buffer, position, unscaled.longValue(), 8);
      position += 8;
    } else {
      writePrimitiveHeader(TYPE_DECIMAL16, 17);
      buffer[position++] = (byte) scale;
      byte[] bigEndian = unscaled.toByteArray();
      byte signExtension = (byte) (unscaled.signum() < 0 ? 0xFF : 0x00);
      for (int i = 0; i < 16; i++) {
        int index = bigEndian.length - 1 - i;
        buffer[position + i] = index >= 0 ? bigEndian[index] : signExtension;
      }
      position += 16;
    }
  }

  /** Appends a string, using the one-byte "short string" header when it is under 64 bytes. */
  void appendString(String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (bytes.length <= MAX_SHORT_STRING_BYTES) {
      ensureCapacity((long) position + 1 + bytes.length);
      buffer[position++] = shortStringHeader(bytes.length);
    } else {
      writePrimitiveHeader(TYPE_STRING, 4 + (long) bytes.length);
      writeLittleEndian(buffer, position, bytes.length, 4);
      position += 4;
    }
    System.arraycopy(bytes, 0, buffer, position, bytes.length);
    position += bytes.length;
  }

  void appendBinary(byte[] bytes) {
    writePrimitiveHeader(TYPE_BINARY, 4 + (long) bytes.length);
    writeLittleEndian(buffer, position, bytes.length, 4);
    position += 4;
    System.arraycopy(bytes, 0, buffer, position, bytes.length);
    position += bytes.length;
  }

  void appendDate(LocalDate date) {
    long days = date.toEpochDay();
    if (days != (int) days) {
      throw new IllegalArgumentException("Date " + date + " is out of range for a Variant date");
    }
    writePrimitive(TYPE_DATE, days, 4);
  }

  /** Appends a time of day. Variant times have microsecond precision, so nanos are truncated. */
  void appendTime(LocalTime time) {
    writePrimitive(TYPE_TIME, time.toNanoOfDay() / 1000, 8);
  }

  /**
   * Appends an instant as a timestamp with time zone. Microsecond precision is used when it is
   * lossless; otherwise the nanosecond type is used.
   */
  void appendTimestamp(Instant instant) {
    appendTimestamp(instant.getEpochSecond(), instant.getNano(), true);
  }

  /** Appends a local date-time as a timestamp without time zone. */
  void appendTimestampNtz(LocalDateTime dateTime) {
    Instant asUtc = dateTime.toInstant(ZoneOffset.UTC);
    appendTimestamp(asUtc.getEpochSecond(), asUtc.getNano(), false);
  }

  private void appendTimestamp(long epochSecond, int nano, boolean withTimeZone) {
    try {
      if (nano % 1000 == 0) {
        long micros = Math.addExact(Math.multiplyExact(epochSecond, 1_000_000L), nano / 1000);
        writePrimitive(withTimeZone ? TYPE_TIMESTAMP : TYPE_TIMESTAMP_NTZ, micros, 8);
      } else {
        long nanos = Math.addExact(Math.multiplyExact(epochSecond, 1_000_000_000L), nano);
        writePrimitive(withTimeZone ? TYPE_TIMESTAMP_NANOS : TYPE_TIMESTAMP_NTZ_NANOS, nanos, 8);
      }
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException("Timestamp is out of range for a Variant timestamp", e);
    }
  }

  void appendUuid(UUID uuid) {
    writePrimitiveHeader(TYPE_UUID, 16);
    // UUIDs are the one big-endian type in the spec.
    long msb = uuid.getMostSignificantBits();
    long lsb = uuid.getLeastSignificantBits();
    for (int i = 0; i < 8; i++) {
      buffer[position + i] = (byte) (msb >>> (56 - 8 * i));
      buffer[position + 8 + i] = (byte) (lsb >>> (56 - 8 * i));
    }
    position += 16;
  }

  /**
   * Completes an object whose field values were appended starting at {@code start}. Each entry
   * holds a field's dictionary ID and the offset of its value relative to {@code start}.
   */
  void finishObject(int start, List<FieldEntry> fields) {
    fields.sort((a, b) -> compareUtf8(dictionaryKeys.get(a.id), dictionaryKeys.get(b.id)));
    int maxId = 0;
    for (int i = 0; i < fields.size(); i++) {
      FieldEntry field = fields.get(i);
      if (i > 0 && fields.get(i - 1).id == field.id) {
        throw new IllegalArgumentException(
            "Duplicate object key: "
                + new String(dictionaryKeys.get(field.id), StandardCharsets.UTF_8));
      }
      maxId = Math.max(maxId, field.id);
    }
    int numFields = fields.size();
    int dataSize = position - start;
    boolean isLarge = numFields > 0xFF;
    int idSize = sizeOf(maxId);
    int offsetSize = sizeOf(dataSize);
    int headerSize = 1 + (isLarge ? 4 : 1) + numFields * idSize + (numFields + 1) * offsetSize;
    shiftData(start, dataSize, headerSize);

    int p = start;
    buffer[p++] = objectHeader(isLarge, idSize, offsetSize);
    writeLittleEndian(buffer, p, numFields, isLarge ? 4 : 1);
    p += isLarge ? 4 : 1;
    for (FieldEntry field : fields) {
      writeLittleEndian(buffer, p, field.id, idSize);
      p += idSize;
    }
    for (FieldEntry field : fields) {
      writeLittleEndian(buffer, p, field.offset, offsetSize);
      p += offsetSize;
    }
    writeLittleEndian(buffer, p, dataSize, offsetSize);
  }

  /**
   * Completes an array whose elements were appended starting at {@code start}. Each offset is the
   * start of an element relative to {@code start}.
   */
  void finishArray(int start, List<Integer> offsets) {
    int numElements = offsets.size();
    int dataSize = position - start;
    boolean isLarge = numElements > 0xFF;
    int offsetSize = sizeOf(dataSize);
    int headerSize = 1 + (isLarge ? 4 : 1) + (numElements + 1) * offsetSize;
    shiftData(start, dataSize, headerSize);

    int p = start;
    buffer[p++] = arrayHeader(isLarge, offsetSize);
    writeLittleEndian(buffer, p, numElements, isLarge ? 4 : 1);
    p += isLarge ? 4 : 1;
    for (int offset : offsets) {
      writeLittleEndian(buffer, p, offset, offsetSize);
      p += offsetSize;
    }
    writeLittleEndian(buffer, p, dataSize, offsetSize);
  }

  /**
   * Appends an existing Variant. Objects and arrays are rebuilt because their field IDs point into
   * a different metadata dictionary. All other values are copied byte for byte.
   */
  void appendVariant(Variant variant, int depth) {
    switch (variant.getType()) {
      case OBJECT:
        {
          checkNestingDepth(depth);
          int start = position;
          int numFields = variant.numFields();
          List<FieldEntry> fields = new ArrayList<>(numFields);
          for (int i = 0; i < numFields; i++) {
            fields.add(new FieldEntry(addKey(variant.fieldNameAt(i)), position - start));
            appendVariant(variant.fieldValueAt(i), depth + 1);
          }
          finishObject(start, fields);
          return;
        }
      case ARRAY:
        {
          checkNestingDepth(depth);
          int start = position;
          int numElements = variant.numElements();
          List<Integer> offsets = new ArrayList<>(numElements);
          for (int i = 0; i < numElements; i++) {
            offsets.add(position - start);
            appendVariant(variant.getElement(i), depth + 1);
          }
          finishArray(start, offsets);
          return;
        }
      default:
        int size = variant.valueSize();
        ensureCapacity((long) position + size);
        System.arraycopy(variant.value, variant.pos, buffer, position, size);
        position += size;
    }
  }

  /** Appends a plain Java value. See {@link Variant#of(Object)} for the supported types. */
  void appendJava(@Nullable Object value, int depth) {
    if (value == null) {
      appendNull();
    } else if (value instanceof Variant) {
      appendVariant((Variant) value, depth);
    } else if (value instanceof Boolean) {
      appendBoolean((Boolean) value);
    } else if (value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long) {
      appendLong(((Number) value).longValue());
    } else if (value instanceof BigInteger) {
      appendDecimal(new BigDecimal((BigInteger) value));
    } else if (value instanceof BigDecimal) {
      appendDecimal((BigDecimal) value);
    } else if (value instanceof Float) {
      appendFloat((Float) value);
    } else if (value instanceof Double) {
      appendDouble((Double) value);
    } else if (value instanceof CharSequence) {
      appendString(value.toString());
    } else if (value instanceof byte[]) {
      appendBinary((byte[]) value);
    } else if (value instanceof ByteBuffer) {
      ByteBuffer bytes = ((ByteBuffer) value).duplicate();
      byte[] copy = new byte[bytes.remaining()];
      bytes.get(copy);
      appendBinary(copy);
    } else if (value instanceof LocalDate) {
      appendDate((LocalDate) value);
    } else if (value instanceof LocalTime) {
      appendTime((LocalTime) value);
    } else if (value instanceof LocalDateTime) {
      appendTimestampNtz((LocalDateTime) value);
    } else if (value instanceof Instant) {
      appendTimestamp((Instant) value);
    } else if (value instanceof ReadableInstant) {
      appendTimestamp(Instant.ofEpochMilli(((ReadableInstant) value).getMillis()));
    } else if (value instanceof UUID) {
      appendUuid((UUID) value);
    } else if (value instanceof Map) {
      checkNestingDepth(depth);
      int start = position;
      Map<?, ?> map = (Map<?, ?>) value;
      List<FieldEntry> fields = new ArrayList<>(map.size());
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object key = entry.getKey();
        if (!(key instanceof CharSequence)) {
          throw new IllegalArgumentException(
              "Variant object keys must be strings, but got: "
                  + (key == null ? "null" : key.getClass().getName()));
        }
        fields.add(new FieldEntry(addKey(key.toString()), position - start));
        appendJava(entry.getValue(), depth + 1);
      }
      finishObject(start, fields);
    } else if (value instanceof Iterable || value instanceof Object[]) {
      checkNestingDepth(depth);
      Iterable<?> elements =
          value instanceof Object[] ? Arrays.asList((Object[]) value) : (Iterable<?>) value;
      int start = position;
      List<Integer> offsets = new ArrayList<>();
      for (Object element : elements) {
        offsets.add(position - start);
        appendJava(element, depth + 1);
      }
      finishArray(start, offsets);
    } else {
      throw new IllegalArgumentException(
          "Cannot convert a value of type " + value.getClass().getName() + " to a Variant");
    }
  }

  /** Returns the finished Variant. The encoder must not be used afterwards. */
  Variant build() {
    int dictionarySize = dictionaryKeys.size();
    boolean sorted = dictionarySize > 0;
    for (int i = 1; i < dictionarySize && sorted; i++) {
      sorted = compareUtf8(dictionaryKeys.get(i - 1), dictionaryKeys.get(i)) < 0;
    }
    int offsetSize = sizeOf(Math.max(dictionarySize, dictionaryBytes));
    long metadataSize = 1 + offsetSize + (dictionarySize + 1L) * offsetSize + dictionaryBytes;
    if (metadataSize > MAX_SIZE) {
      throw new IllegalArgumentException(
          "Variant metadata would exceed the maximum size of " + MAX_SIZE + " bytes");
    }
    byte[] metadata = new byte[(int) metadataSize];
    metadata[0] = (byte) (VERSION | (sorted ? SORTED_STRINGS_BIT : 0) | ((offsetSize - 1) << 6));
    writeLittleEndian(metadata, 1, dictionarySize, offsetSize);
    int offsetsPos = 1 + offsetSize;
    int stringsPos = offsetsPos + (dictionarySize + 1) * offsetSize;
    int offset = 0;
    for (int i = 0; i < dictionarySize; i++) {
      byte[] key = dictionaryKeys.get(i);
      writeLittleEndian(metadata, offsetsPos + i * offsetSize, offset, offsetSize);
      System.arraycopy(key, 0, metadata, stringsPos + offset, key.length);
      offset += key.length;
    }
    writeLittleEndian(metadata, offsetsPos + dictionarySize * offsetSize, offset, offsetSize);
    return Variant.wrap(metadata, Arrays.copyOf(buffer, position));
  }

  private void writePrimitiveHeader(int typeId, long payloadSize) {
    ensureCapacity(position + 1 + payloadSize);
    buffer[position++] = primitiveHeader(typeId);
  }

  private void writePrimitive(int typeId, long payload, int payloadSize) {
    writePrimitiveHeader(typeId, payloadSize);
    writeLittleEndian(buffer, position, payload, payloadSize);
    position += payloadSize;
  }

  /** Moves {@code [start, start + length)} right by {@code distance} bytes to make room. */
  private void shiftData(int start, int length, int distance) {
    ensureCapacity((long) position + distance);
    System.arraycopy(buffer, start, buffer, start + distance, length);
    position += distance;
  }

  private void ensureCapacity(long required) {
    if (required > MAX_SIZE) {
      throw new IllegalArgumentException(
          "Variant value would exceed the maximum size of " + MAX_SIZE + " bytes");
    }
    if (required > buffer.length) {
      long newSize = Math.min(MAX_SIZE, Math.max(required, 2L * buffer.length));
      buffer = Arrays.copyOf(buffer, (int) newSize);
    }
  }
}
