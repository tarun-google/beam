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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.variant.Variant;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A logical type for semi-structured {@link Variant} values.
 *
 * <p>The base type is {@code ROW<metadata: BYTES, value: BYTES>}. Both fields hold bytes in the
 * Apache Parquet Variant binary encoding, the same layout used by Parquet, Apache Iceberg, Apache
 * Spark and Apache Flink. This is a standard portable logical type, so every Beam SDK and runner
 * can carry it. An SDK that does not know the type still sees a plain row of two byte fields.
 *
 * <p>Use the shared instance {@link SqlTypes#VARIANT} in schemas:
 *
 * <pre>{@code
 * Schema schema = Schema.builder()
 *     .addInt64Field("id")
 *     .addLogicalTypeField("payload", SqlTypes.VARIANT)
 *     .build();
 * Row row = Row.withSchema(schema)
 *     .addValues(1L, Variant.parseJson("{\"event\": \"login\"}"))
 *     .build();
 * }</pre>
 */
public class VariantLogicalType implements Schema.LogicalType<Variant, Row> {
  public static final String IDENTIFIER =
      SchemaApi.LogicalTypes.Enum.VARIANT
          .getValueDescriptor()
          .getOptions()
          .getExtension(RunnerApi.beamUrn);

  public static final String METADATA_FIELD = "metadata";
  public static final String VALUE_FIELD = "value";

  private static final Schema BASE_SCHEMA =
      Schema.builder().addByteArrayField(METADATA_FIELD).addByteArrayField(VALUE_FIELD).build();

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Schema.@Nullable FieldType getArgumentType() {
    return null;
  }

  @Override
  public Schema.FieldType getBaseType() {
    return Schema.FieldType.row(BASE_SCHEMA);
  }

  @Override
  public Row toBaseType(Variant input) {
    return Row.withSchema(BASE_SCHEMA).addValues(input.getMetadata(), input.getValue()).build();
  }

  @Override
  public Variant toInputType(Row base) {
    return Variant.fromBytes(
        checkArgumentNotNull(base.getBytes(0), "Variant row is missing the metadata field"),
        checkArgumentNotNull(base.getBytes(1), "Variant row is missing the value field"));
  }
}
