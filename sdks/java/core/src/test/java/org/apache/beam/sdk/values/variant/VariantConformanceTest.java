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

import static org.apache.beam.sdk.values.variant.VariantTest.hex;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Checks that Beam reads the official Variant test vectors from <a
 * href="https://github.com/apache/parquet-testing/tree/master/variant">apache/parquet-testing</a>
 * exactly. The vectors were written by Apache Spark and Apache Iceberg, so passing them shows Beam
 * reads the same bytes those engines write.
 */
@RunWith(JUnit4.class)
public class VariantConformanceTest {
  private static final String VECTORS = "parquet_testing_variant_vectors.txt";

  /** Vector name to {metadata, value} bytes. */
  private static final Map<String, byte[][]> RAW = new LinkedHashMap<>();

  @BeforeClass
  public static void loadVectors() throws IOException {
    InputStream stream = VariantConformanceTest.class.getResourceAsStream(VECTORS);
    assertNotNull("Missing test resource " + VECTORS, stream);
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        String[] parts = line.split(" ", -1);
        RAW.put(parts[0], new byte[][] {hex(parts[1]), hex(parts[2])});
      }
    }
    assertEquals(29, RAW.size());
  }

  private static Variant vector(String name) {
    byte[][] raw = RAW.get(name);
    assertNotNull("Unknown vector " + name, raw);
    return Variant.fromBytes(raw[0], raw[1]);
  }

  @Test
  public void testEveryVectorDecodesAndRoundTrips() {
    for (Map.Entry<String, byte[][]> entry : RAW.entrySet()) {
      String name = entry.getKey();
      Variant variant = Variant.fromBytes(entry.getValue()[0], entry.getValue()[1]);
      // The whole value buffer is one value, so sizing it must consume every byte.
      assertArrayEquals(name, entry.getValue()[1], variant.getValue());
      assertNotNull(name, variant.toJson());
      // Re-encoding with Beam's encoder gives a logically equal value.
      assertEquals(name, variant, Variant.of(variant));
      assertEquals(name, variant.hashCode(), Variant.of(variant).hashCode());
    }
  }

  @Test
  public void testPrimitives() {
    assertTrue(vector("primitive_null").isNull());
    assertTrue(vector("primitive_boolean_true").getBoolean());
    assertFalse(vector("primitive_boolean_false").getBoolean());

    assertTypeAndLong(Variant.Type.INT8, 42, vector("primitive_int8"));
    assertTypeAndLong(Variant.Type.INT16, 1234, vector("primitive_int16"));
    assertTypeAndLong(Variant.Type.INT32, 123456, vector("primitive_int32"));
    assertTypeAndLong(Variant.Type.INT64, 1234567890123456789L, vector("primitive_int64"));

    assertEquals(Variant.Type.FLOAT, vector("primitive_float").getType());
    assertEquals(1.23456794E9f, vector("primitive_float").getFloat(), 0f);
    assertEquals(Variant.Type.DOUBLE, vector("primitive_double").getType());
    assertEquals(1234567890.1234, vector("primitive_double").getDouble(), 0d);

    assertTypeAndDecimal(Variant.Type.DECIMAL4, "12.34", vector("primitive_decimal4"));
    assertTypeAndDecimal(Variant.Type.DECIMAL8, "12345678.90", vector("primitive_decimal8"));
    assertTypeAndDecimal(
        Variant.Type.DECIMAL16, "12345678912345678.90", vector("primitive_decimal16"));

    assertEquals(LocalDate.parse("2025-04-16"), vector("primitive_date").getDate());
    assertEquals(LocalTime.parse("12:33:54.123456"), vector("primitive_time").getTime());
    assertEquals(
        Instant.parse("2025-04-16T16:34:56.780Z"), vector("primitive_timestamp").getTimestamp());
    assertEquals(
        LocalDateTime.parse("2025-04-16T12:34:56.780"),
        vector("primitive_timestampntz").getTimestampNtz());
    assertEquals(Variant.Type.TIMESTAMP_NANOS, vector("primitive_timestamp_nanos").getType());
    assertEquals(
        Instant.parse("2024-11-07T12:33:54.123456789Z"),
        vector("primitive_timestamp_nanos").getTimestamp());
    assertEquals(
        Variant.Type.TIMESTAMP_NTZ_NANOS, vector("primitive_timestampntz_nanos").getType());
    assertEquals(
        LocalDateTime.parse("2024-11-07T12:33:54.123456789"),
        vector("primitive_timestampntz_nanos").getTimestampNtz());
    assertEquals(
        UUID.fromString("f24f9b64-81fa-49d1-b74e-8c09a6e31c56"),
        vector("primitive_uuid").getUuid());
    assertArrayEquals(hex("031337deadbeefcafe"), vector("primitive_binary").getBinary());
  }

  @Test
  public void testStrings() {
    assertEquals("Less than 64 bytes (❤️ with utf8)", vector("short_string").getString());

    String longString = vector("primitive_string").getString();
    assertTrue(longString.startsWith("This string is longer than 64 bytes"));
    assertTrue(longString.contains("🐢"));

    assertTrue(vector("long_string").getString().startsWith("This string is for sure"));
  }

  @Test
  public void testObjects() {
    assertEquals("{}", vector("object_empty").toJson());

    Variant nested = vector("object_nested");
    assertEquals(
        "{\"id\":1,\"observation\":{\"location\":\"In the Volcano\",\"time\":\"12:34:56\","
            + "\"value\":{\"humidity\":456,\"temperature\":123}},"
            + "\"species\":{\"name\":\"lava monster\",\"population\":6789}}",
        nested.toJson());
    assertEquals(456, nested.get("$.observation.value.humidity").getInt());
    assertEquals("lava monster", nested.get("$.species.name").getString());

    Variant primitives = vector("object_primitive");
    assertEquals(1, primitives.get("$.int_field").getInt());
    assertEquals(1.23456789, primitives.get("$.double_field").getDouble(), 0d);
    assertTrue(primitives.get("$.boolean_true_field").getBoolean());
    assertFalse(primitives.get("$.boolean_false_field").getBoolean());
    assertTrue(primitives.get("$.null_field").isNull());
    assertEquals("Apache Parquet", primitives.get("$.string_field").getString());
    assertNotNull(primitives.get("$.timestamp_field"));
  }

  @Test
  public void testArrays() {
    assertEquals("[]", vector("array_empty").toJson());
    assertEquals("[2,1,5,9]", vector("array_primitive").toJson());

    Variant nested = vector("array_nested");
    assertEquals(
        "[{\"id\":1,\"thing\":{\"names\":[\"Contrarian\",\"Spider\"]}},null,"
            + "{\"id\":2,\"names\":[\"Apple\",\"Ray\",null],\"type\":\"if\"}]",
        nested.toJson());
    assertEquals("Spider", nested.get("$[0].thing.names[1]").getString());
    assertTrue(nested.get("$[1]").isNull());
    assertTrue(nested.get("$[2].names[2]").isNull());
  }

  private static void assertTypeAndLong(Variant.Type type, long expected, Variant variant) {
    assertEquals(type, variant.getType());
    assertEquals(expected, variant.getLong());
  }

  private static void assertTypeAndDecimal(Variant.Type type, String expected, Variant variant) {
    assertEquals(type, variant.getType());
    assertEquals(new BigDecimal(expected), variant.getDecimal());
  }
}
