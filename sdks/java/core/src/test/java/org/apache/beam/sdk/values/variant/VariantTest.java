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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Variant}. */
@RunWith(JUnit4.class)
public class VariantTest {

  // ---------------------------------------------------------------------------------------------
  // Encoding

  @Test
  public void testEncodesSpecExampleByteForByte() {
    Variant variant = Variant.parseJson("{\"event\":\"login\",\"n\":7}");

    // Dictionary ["event", "n"], sorted, 1-byte offsets.
    assertArrayEquals(hex("11 02 00 05 06 65 76 65 6E 74 6E"), variant.getMetadata());
    // Object with 2 fields: ids [0, 1], offsets [0, 6, 8], then "login" and int8 7.
    assertArrayEquals(hex("02 02 00 01 00 06 08 15 6C 6F 67 69 6E 0C 07"), variant.getValue());
  }

  @Test
  public void testIntegersUseSmallestType() {
    assertEquals(Variant.Type.INT8, Variant.of(42).getType());
    assertEquals(Variant.Type.INT8, Variant.of(-128).getType());
    assertEquals(Variant.Type.INT16, Variant.of(128).getType());
    assertEquals(Variant.Type.INT16, Variant.of(300).getType());
    assertEquals(Variant.Type.INT32, Variant.of(70_000).getType());
    assertEquals(Variant.Type.INT64, Variant.of(1L << 40).getType());
    assertEquals(1L << 40, Variant.of(1L << 40).getLong());
    assertEquals(-128, Variant.of(-128).getInt());
  }

  @Test
  public void testDecimalsUseSmallestType() {
    assertEquals(Variant.Type.DECIMAL4, Variant.of(new BigDecimal("1234567.89")).getType());
    assertEquals(Variant.Type.DECIMAL8, Variant.of(new BigDecimal("12345678.901")).getType());
    assertEquals(
        Variant.Type.DECIMAL16, Variant.of(new BigDecimal("12345678901234567890.5")).getType());

    BigDecimal negative = new BigDecimal("-98765432109876543210.123456789");
    assertEquals(negative, Variant.of(negative).getDecimal());
    // A negative scale is widened to 0, which is exact.
    assertEquals(new BigDecimal("1000"), Variant.of(new BigDecimal("1E+3")).getDecimal());
    assertThrows(
        IllegalArgumentException.class,
        () -> Variant.of(new BigDecimal("1234567890123456789012345678901234567890")));
  }

  @Test
  public void testPrimitiveRoundTrips() {
    assertTrue(Variant.ofNull().isNull());
    assertEquals(Variant.Type.NULL, Variant.of(null).getType());
    assertTrue(Variant.of(true).getBoolean());
    assertFalse(Variant.of(false).getBoolean());
    assertEquals(1.5f, Variant.of(1.5f).getFloat(), 0f);
    assertEquals(Math.PI, Variant.of(Math.PI).getDouble(), 0d);
    assertEquals("héllo", Variant.of("héllo").getString());
    assertArrayEquals(new byte[] {1, 2, 3}, Variant.of(new byte[] {1, 2, 3}).getBinary());

    LocalDate date = LocalDate.of(2025, 4, 16);
    assertEquals(date, Variant.of(date).getDate());
    LocalTime time = LocalTime.of(12, 33, 54, 123_456_000);
    assertEquals(time, Variant.of(time).getTime());
    UUID uuid = UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56");
    assertEquals(uuid, Variant.of(uuid).getUuid());
  }

  @Test
  public void testTimestampsKeepPrecision() {
    Instant micros = Instant.parse("2025-04-16T16:34:56.780123Z");
    assertEquals(Variant.Type.TIMESTAMP, Variant.of(micros).getType());
    assertEquals(micros, Variant.of(micros).getTimestamp());

    Instant nanos = Instant.parse("2024-11-07T12:33:54.123456789Z");
    assertEquals(Variant.Type.TIMESTAMP_NANOS, Variant.of(nanos).getType());
    assertEquals(nanos, Variant.of(nanos).getTimestamp());

    Instant beforeEpoch = Instant.parse("1969-12-31T23:59:59.5Z");
    assertEquals(beforeEpoch, Variant.of(beforeEpoch).getTimestamp());

    LocalDateTime local = LocalDateTime.parse("2025-04-16T12:34:56.780");
    assertEquals(Variant.Type.TIMESTAMP_NTZ, Variant.of(local).getType());
    assertEquals(local, Variant.of(local).getTimestampNtz());

    LocalDateTime localNanos = LocalDateTime.parse("2024-11-07T12:33:54.123456789");
    assertEquals(Variant.Type.TIMESTAMP_NTZ_NANOS, Variant.of(localNanos).getType());
    assertEquals(localNanos, Variant.of(localNanos).getTimestampNtz());
  }

  @Test
  public void testShortAndLongStrings() {
    String shortString = repeat('a', 63);
    String longString = repeat('a', 64);
    // Basic type 1 is the one-byte "short string" header; longer strings use primitive type 16.
    assertEquals(1, Variant.of(shortString).getValue()[0] & 0x3);
    assertEquals(0, Variant.of(longString).getValue()[0] & 0x3);
    assertEquals(Variant.Type.STRING, Variant.of(shortString).getType());
    assertEquals(Variant.Type.STRING, Variant.of(longString).getType());
    assertEquals(longString, Variant.of(longString).getString());
    assertEquals(Variant.of(shortString + "a"), Variant.of(longString));
  }

  // ---------------------------------------------------------------------------------------------
  // JSON

  @Test
  public void testJsonNumbersStayExact() {
    Variant amount = Variant.parseJson("{\"amount\": 19.99}").getField("amount");
    assertEquals(Variant.Type.DECIMAL4, amount.getType());
    assertEquals(new BigDecimal("19.99"), amount.getDecimal());

    assertEquals(Variant.Type.DOUBLE, Variant.parseJson("1e3").getType());
    assertEquals(1000d, Variant.parseJson("1e3").getDouble(), 0d);

    Variant big = Variant.parseJson("12345678901234567890123");
    assertEquals(Variant.Type.DECIMAL16, big.getType());
    assertEquals(new BigDecimal("12345678901234567890123"), big.getDecimal());

    // More than 38 digits cannot be a decimal, so it becomes a double.
    assertEquals(
        Variant.Type.DOUBLE,
        Variant.parseJson("123456789012345678901234567890123456789012").getType());
  }

  @Test
  public void testJsonRoundTripSortsKeys() {
    Variant variant =
        Variant.parseJson(
            "{\"b\": [1, 2.5, \"x\", null, true], \"a\": {\"z\": \"é\\n\", \"y\": {}}, \"c\": []}");
    assertEquals(
        "{\"a\":{\"y\":{},\"z\":\"é\\n\"},\"b\":[1,2.5,\"x\",null,true],\"c\":[]}",
        variant.toJson());
    assertEquals(variant, Variant.parseJson(variant.toJson()));
  }

  @Test
  public void testNonJsonTypesAreWrittenAsStrings() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("date", LocalDate.of(2025, 4, 16));
    map.put("ts", Instant.parse("2025-04-16T16:34:56.780Z"));
    map.put("bin", new byte[] {3, 19, 55});
    map.put("id", UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56"));
    assertEquals(
        "{\"bin\":\"AxM3\",\"date\":\"2025-04-16\","
            + "\"id\":\"f24f9b64-81fa-49d1-b74e-8c09a6e31c56\",\"ts\":\"2025-04-16T16:34:56.780Z\"}",
        Variant.of(map).toJson());
  }

  @Test
  public void testInvalidJson() {
    assertThrows(IllegalArgumentException.class, () -> Variant.parseJson("{"));
    assertThrows(IllegalArgumentException.class, () -> Variant.parseJson(""));
    assertThrows(IllegalArgumentException.class, () -> Variant.parseJson("1 2"));
    assertThrows(IllegalArgumentException.class, () -> Variant.parseJson("{\"a\": 1, \"a\": 2}"));
    assertNull(Variant.tryParseJson("not json"));
    assertNull(Variant.tryParseJson("{\"a\": 1, \"a\": 2}"));
    assertNull(Variant.tryParseJson(null));
    assertEquals(Variant.of(1), Variant.tryParseJson("1"));
  }

  // ---------------------------------------------------------------------------------------------
  // Objects, arrays and paths

  @Test
  public void testLargeObjectLookup() {
    Map<String, Object> fields = new LinkedHashMap<>();
    for (int i = 499; i >= 0; i--) {
      fields.put("field_" + i, i);
    }
    Variant variant = Variant.of(fields);

    // More than 255 fields sets the is_large bit (bit 6 of the object header).
    assertTrue((variant.getValue()[0] & 0x40) != 0);
    assertEquals(500, variant.numFields());
    for (int i = 0; i < 500; i++) {
      assertEquals(i, variant.getField("field_" + i).getInt());
    }
    assertNull(variant.getField("field_500"));
    assertNull(variant.getField(""));
  }

  @Test
  public void testFieldNamesAreSortedByUtf8Bytes() {
    Map<String, Object> fields = new LinkedHashMap<>();
    fields.put("z", 1);
    fields.put("é", 2);
    fields.put("a", 3);
    fields.put("Z", 4);
    fields.put("🐢", 5);
    Variant variant = Variant.of(fields);

    // Unsigned UTF-8 byte order: 'Z' (5A) < 'a' (61) < 'z' (7A) < 'é' (C3 A9) < '🐢' (F0 ...).
    assertEquals(Arrays.asList("Z", "a", "z", "é", "🐢"), variant.getFieldNames());
    assertEquals(2, variant.getField("é").getInt());
    assertEquals(5, variant.getField("🐢").getInt());
  }

  @Test
  public void testPathAccess() {
    Variant variant =
        Variant.parseJson(
            "{\"user\": {\"name\": \"ann\", \"tags\": [\"x\", \"y\"], \"key.with.dot\": 1,"
                + " \"quote's\": true}}");

    assertEquals("ann", variant.get("$.user.name").getString());
    assertEquals("y", variant.get("$.user.tags[1]").getString());
    assertEquals(1, variant.get("$['user']['key.with.dot']").getInt());
    assertTrue(variant.get("$[\"user\"]['quote\\'s']").getBoolean());
    assertEquals(variant, variant.get("$"));

    assertNull(variant.get("$.user.missing"));
    assertNull(variant.get("$.user.tags[5]"));
    assertNull(variant.get("$.user.name.first")); // field step on a string
    assertNull(variant.get("$.user[0]")); // index step on an object

    for (String invalid : Arrays.asList("user.name", "$.", "$[abc]", "$['x", "$x", "$[1")) {
      assertThrows(invalid, IllegalArgumentException.class, () -> variant.get(invalid));
    }
  }

  @Test
  public void testNestedValuesAreSelfContained() {
    Variant variant = Variant.parseJson("{\"device\": {\"os\": \"ios\", \"v\": [1, 2]}, \"x\": 1}");
    Variant device = variant.get("$.device");

    // A nested value plus the document's metadata is a valid standalone Variant.
    Variant copy = Variant.fromBytes(device.getMetadata(), device.getValue());
    assertEquals(device, copy);
    assertEquals("ios", copy.getField("os").getString());
    assertEquals("{\"os\":\"ios\",\"v\":[1,2]}", copy.toJson());
  }

  @Test
  public void testJavaCollections() {
    Map<String, Object> address = new LinkedHashMap<>();
    address.put("zip", "94043");
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("name", "ann");
    map.put("scores", Arrays.asList(1, 2.5, null));
    map.put("address", address);
    map.put("array", new Object[] {"a", true});
    Variant variant = Variant.of(map);

    assertEquals(Variant.Type.OBJECT, variant.getType());
    assertEquals("94043", variant.get("$.address.zip").getString());
    assertEquals(3, variant.get("$.scores").numElements());
    assertTrue(variant.get("$.scores[2]").isNull());
    assertTrue(variant.get("$.array[1]").getBoolean());

    assertThrows(
        IllegalArgumentException.class, () -> Variant.of(Collections.singletonMap(1, "a")));
    assertThrows(IllegalArgumentException.class, () -> Variant.of(new Object()));
  }

  @Test
  public void testEmbeddedVariantIsReencoded() {
    Variant inner = Variant.parseJson("{\"x\": 1, \"y\": [\"deep\"]}");
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("a", "first");
    map.put("inner", inner);
    Variant outer = Variant.of(map);

    // The inner object's field IDs are remapped into the outer dictionary.
    assertEquals("deep", outer.get("$.inner.y[0]").getString());
    assertEquals(inner, outer.get("$.inner"));
    assertEquals(
        Variant.parseJson("{\"a\":\"first\",\"inner\":{\"x\":1,\"y\":[\"deep\"]}}"), outer);
  }

  // ---------------------------------------------------------------------------------------------
  // Equality, type checks and serialization

  @Test
  public void testLogicalEquality() {
    Variant a = Variant.parseJson("{\"a\": 1, \"b\": [1, 2]}");
    Variant b = Variant.parseJson("{\"b\": [1, 2], \"a\": 1}");
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());

    // Integers and decimals are one equivalence class.
    assertEquals(Variant.of(1), Variant.of(new BigDecimal("1.00")));
    assertEquals(Variant.of(1).hashCode(), Variant.of(new BigDecimal("1.00")).hashCode());
    assertEquals(Variant.of(7), Variant.of(7L));

    // Doubles, strings and integers are different classes.
    assertNotEquals(Variant.of(1), Variant.of(1.0d));
    assertNotEquals(Variant.of(1), Variant.of("1"));
    assertNotEquals(Variant.parseJson("[1, 2]"), Variant.parseJson("[2, 1]"));

    // The same instant at micro and nano precision is one value.
    Variant micros = Variant.fromBytes(hex("01 00 00"), hex("30 40 42 0F 00 00 00 00 00"));
    Variant nanos = Variant.fromBytes(hex("01 00 00"), hex("48 00 CA 9A 3B 00 00 00 00"));
    assertEquals(Variant.Type.TIMESTAMP, micros.getType());
    assertEquals(Variant.Type.TIMESTAMP_NANOS, nanos.getType());
    assertEquals(micros, nanos);
    assertEquals(micros.hashCode(), nanos.hashCode());
  }

  @Test
  public void testAccessorTypeMismatch() {
    assertThrows(IllegalStateException.class, () -> Variant.of("s").getLong());
    assertThrows(IllegalStateException.class, () -> Variant.of(1).getString());
    assertThrows(IllegalStateException.class, () -> Variant.of(1).getField("a"));
    assertThrows(IllegalStateException.class, () -> Variant.of(1).getElement(0));
    assertThrows(IndexOutOfBoundsException.class, () -> Variant.parseJson("[1]").getElement(5));
    assertThrows(ArithmeticException.class, () -> Variant.of(1L << 40).getInt());
    assertEquals(19.99, Variant.parseJson("19.99").getDouble(), 0d);
  }

  @Test
  public void testJavaSerialization() {
    Variant variant = Variant.parseJson("{\"a\": {\"b\": [1, \"two\"]}}");
    assertEquals(variant, SerializableUtils.ensureSerializable(variant));
    Variant nested = variant.get("$.a.b");
    assertEquals(nested, SerializableUtils.ensureSerializable(nested));
  }

  // ---------------------------------------------------------------------------------------------
  // Malformed and hostile input

  @Test
  public void testMalformedInputIsRejectedCleanly() {
    // Unsupported metadata version.
    assertThrows(
        IllegalArgumentException.class, () -> Variant.fromBytes(hex("02 00 00"), hex("00")));
    // Empty value.
    assertThrows(
        IllegalArgumentException.class, () -> Variant.fromBytes(hex("01 00 00"), new byte[0]));
    // Truncated metadata offsets.
    assertThrows(
        IllegalArgumentException.class, () -> Variant.fromBytes(hex("01 05 00"), hex("00")));
    // A long string that claims 255 bytes but has 1.
    assertThrows(
        IllegalArgumentException.class,
        () -> Variant.fromBytes(hex("01 00 00"), hex("40 FF 00 00 00 41")));

    // An object whose field offset points past its data.
    Variant badOffset = Variant.fromBytes(hex("01 01 00 01 61"), hex("02 01 00 05 01 00"));
    assertThrows(IllegalArgumentException.class, () -> badOffset.getField("a"));

    // An object whose field id is outside the dictionary.
    Variant badId = Variant.fromBytes(hex("01 00 00"), hex("02 01 03 00 01 00"));
    assertThrows(IllegalArgumentException.class, badId::getFieldNames);
  }

  @Test
  public void testDeeplyNestedInputIsRejected() {
    // Encoder and JSON parser depth limits.
    Object nested = null;
    for (int i = 0; i < 1100; i++) {
      nested = Collections.singletonList(nested);
    }
    Object deep = nested;
    assertThrows(IllegalArgumentException.class, () -> Variant.of(deep));
    assertThrows(
        IllegalArgumentException.class,
        () -> Variant.parseJson(repeat('[', 1100) + repeat(']', 1100)));

    // Hand-built bytes that nest 1100 single-element arrays, so walking the tree must stop.
    Variant bomb = Variant.fromBytes(hex("01 00 00"), nestedArrayBytes(1100));
    Variant bombCopy = Variant.fromBytes(bomb.getMetadata(), bomb.getValue());
    assertThrows(IllegalArgumentException.class, bomb::toJson);
    assertThrows(IllegalArgumentException.class, bomb::hashCode);
    assertThrows(IllegalArgumentException.class, () -> bomb.equals(bombCopy));
    assertThrows(IllegalArgumentException.class, () -> Variant.of(bomb));
    // toString must never throw, since loggers and assertion messages call it.
    assertTrue(bomb.toString().startsWith("Variant("));
  }

  @Test
  public void testNestingLimitIsExactlyMaxDepth() {
    // 1000 nested arrays are accepted everywhere, matching Jackson, parquet-java and Iceberg.
    String atLimit = repeat('[', 1000) + repeat(']', 1000);
    Variant deepest = Variant.parseJson(atLimit);
    Variant copy = Variant.fromBytes(deepest.getMetadata(), deepest.getValue());
    assertEquals(atLimit, deepest.toJson());
    assertEquals(deepest, copy);
    assertEquals(deepest.hashCode(), copy.hashCode());
    Variant bytesAtLimit = Variant.fromBytes(hex("01 00 00"), nestedArrayBytes(1000));
    assertEquals(repeat('[', 1000) + "null" + repeat(']', 1000), bytesAtLimit.toJson());
    assertEquals(bytesAtLimit.hashCode(), Variant.of(bytesAtLimit).hashCode());

    // One more level is rejected.
    assertThrows(IllegalArgumentException.class, () -> Variant.parseJson("[" + atLimit + "]"));
    assertThrows(
        IllegalArgumentException.class, () -> Variant.of(Collections.singletonList(deepest)));
  }

  @Test
  public void testSizeLimit() {
    assertThrows(IllegalArgumentException.class, () -> Variant.of(new byte[16 * 1024 * 1024]));
  }

  // ---------------------------------------------------------------------------------------------
  // Helpers

  /** Builds {@code depth} single-element arrays around a Variant null, using 4-byte offsets. */
  private static byte[] nestedArrayBytes(int depth) {
    byte[] inner = {0x00};
    for (int i = 0; i < depth; i++) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(0x0F); // array, 4-byte offsets, not large
      out.write(0x01); // one element
      writeIntLe(out, 0);
      writeIntLe(out, inner.length);
      out.write(inner, 0, inner.length);
      inner = out.toByteArray();
    }
    return inner;
  }

  private static void writeIntLe(ByteArrayOutputStream out, int value) {
    for (int i = 0; i < 4; i++) {
      out.write((value >>> (8 * i)) & 0xFF);
    }
  }

  static byte[] hex(String hex) {
    String digits = hex.replace(" ", "");
    byte[] bytes = new byte[digits.length() / 2];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) Integer.parseInt(digits.substring(2 * i, 2 * i + 2), 16);
    }
    return bytes;
  }

  private static String repeat(char c, int count) {
    List<String> parts = new ArrayList<>(Collections.nCopies(count, String.valueOf(c)));
    return String.join("", parts);
  }
}
