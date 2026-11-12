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
package org.apache.beam.sdk.schemas.logicaltypes;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.RowJson;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.variant.Variant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link VariantLogicalType} and how Variant plugs into Beam schemas. */
@RunWith(JUnit4.class)
public class VariantLogicalTypeTest {
  private static final Schema SCHEMA =
      Schema.builder()
          .addInt64Field("id")
          .addLogicalTypeField("payload", SqlTypes.VARIANT)
          .addNullableField("extra", FieldType.logicalType(SqlTypes.VARIANT))
          .build();

  private static final Variant PAYLOAD =
      Variant.parseJson("{\"event\":\"purchase\",\"amount\":19.99,\"items\":[\"a\",\"b\"]}");

  @Test
  public void testIdentifierAndBaseType() {
    assertEquals("beam:logical_type:variant:v1", VariantLogicalType.IDENTIFIER);
    FieldType base = SqlTypes.VARIANT.getBaseType();
    assertEquals(TypeName.ROW, base.getTypeName());
    Schema baseSchema = base.getRowSchema();
    assertEquals(Arrays.asList("metadata", "value"), baseSchema.getFieldNames());
    assertEquals(FieldType.BYTES, baseSchema.getField("metadata").getType());
    assertEquals(FieldType.BYTES, baseSchema.getField("value").getType());
  }

  @Test
  public void testBaseValueIsTheSpecEncodedBytes() {
    Row row = Row.withSchema(SCHEMA).addValues(1L, PAYLOAD, null).build();
    Row base = row.getBaseValue("payload", Row.class);
    assertArrayEquals(PAYLOAD.getMetadata(), base.getBytes("metadata"));
    assertArrayEquals(PAYLOAD.getValue(), base.getBytes("value"));
  }

  @Test
  public void testRowCoderRoundTrip() throws Exception {
    Row row = Row.withSchema(SCHEMA).addValues(1L, PAYLOAD, Variant.of("note")).build();
    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(SCHEMA), row);

    Row decoded = CoderUtils.clone(RowCoder.of(SCHEMA), row);
    Variant payload = decoded.getLogicalTypeValue("payload", Variant.class);
    assertEquals(new BigDecimal("19.99"), payload.get("$.amount").getDecimal());
    assertEquals("b", payload.get("$.items[1]").getString());

    Row withNull = Row.withSchema(SCHEMA).addValues(2L, Variant.ofNull(), null).build();
    CoderProperties.coderDecodeEncodeEqual(RowCoder.of(SCHEMA), withNull);
  }

  @Test
  public void testPortableSchemaRoundTrip() {
    // The portable path, used across SDKs: only the URN and the representation travel.
    SchemaApi.Schema proto = SchemaTranslation.schemaToProto(SCHEMA, false);
    SchemaApi.LogicalType logicalType = proto.getFields(1).getType().getLogicalType();
    assertEquals("beam:logical_type:variant:v1", logicalType.getUrn());
    assertTrue(logicalType.getRepresentation().hasRowType());
    assertTrue(logicalType.getPayload().isEmpty());

    Schema translated = SchemaTranslation.schemaFromProto(proto);
    assertEquals(SCHEMA, translated);
    assertTrue(
        translated.getField("payload").getType().getLogicalType() instanceof VariantLogicalType);
  }

  @Test
  public void testRowProtoRoundTrip() {
    Row row = Row.withSchema(SCHEMA).addValues(1L, PAYLOAD, Variant.ofNull()).build();
    SchemaApi.Row proto = SchemaTranslation.rowToProto(row);
    assertEquals(row, SchemaTranslation.rowFromProto(proto, FieldType.row(SCHEMA)));
  }

  @Test
  public void testPojoFieldIsInferredAsVariant() throws Exception {
    SchemaRegistry registry = SchemaRegistry.createDefault();
    Schema schema = registry.getSchema(Event.class);
    assertEquals(FieldType.logicalType(SqlTypes.VARIANT), schema.getField("payload").getType());

    Event event = new Event();
    event.id = 7;
    event.payload = PAYLOAD;
    Row row = registry.getToRowFunction(Event.class).apply(event);
    assertEquals(PAYLOAD, row.getLogicalTypeValue("payload", Variant.class));

    Event back = registry.getFromRowFunction(Event.class).apply(row);
    assertEquals(7, back.id);
    assertEquals(PAYLOAD, back.payload);
  }

  @Test
  public void testJsonToRowCapturesTheWholeSubtree() {
    Row row =
        jsonToRow(
            "{\"id\": 1, \"payload\": {\"event\": \"purchase\", \"amount\": 19.99,"
                + " \"items\": [\"a\", \"b\"]}}");
    Variant payload = row.getLogicalTypeValue("payload", Variant.class);
    assertEquals(PAYLOAD, payload);
    assertEquals(new BigDecimal("19.99"), payload.get("$.amount").getDecimal());
    assertNull(row.getValue("extra"));

    // ToJson writes the Variant back as nested JSON, not as a quoted string.
    assertEquals(
        "{\"id\":1,\"payload\":{\"amount\":19.99,\"event\":\"purchase\",\"items\":[\"a\",\"b\"]},"
            + "\"extra\":null}",
        rowToJson(row));
  }

  @Test
  public void testJsonPayloadsOfAnyShape() {
    assertEquals(
        Variant.of("just text"),
        jsonToRow("{\"id\": 2, \"payload\": \"just text\"}")
            .getLogicalTypeValue("payload", Variant.class));
    assertEquals(
        Variant.parseJson("[1, {\"a\": null}]"),
        jsonToRow("{\"id\": 3, \"payload\": [1, {\"a\": null}]}")
            .getLogicalTypeValue("payload", Variant.class));

    // JSON null is Variant null in a required field, and SQL null in a nullable one.
    Row nulls = jsonToRow("{\"id\": 4, \"payload\": null, \"extra\": null}");
    assertTrue(nulls.getLogicalTypeValue("payload", Variant.class).isNull());
    assertNull(nulls.getValue("extra"));
  }

  private static Row jsonToRow(String json) {
    ObjectMapper mapper =
        RowJsonUtils.newObjectMapperWith(RowJson.RowJsonDeserializer.forSchema(SCHEMA));
    return RowJsonUtils.jsonToRow(mapper, json);
  }

  private static String rowToJson(Row row) {
    ObjectMapper mapper =
        RowJsonUtils.newObjectMapperWith(RowJson.RowJsonSerializer.forSchema(SCHEMA));
    return RowJsonUtils.rowToJson(mapper, row);
  }

  /** A POJO with a Variant field, for schema inference. */
  @DefaultSchema(JavaFieldSchema.class)
  public static class Event {
    public long id;
    public Variant payload = Variant.ofNull();
  }
}
