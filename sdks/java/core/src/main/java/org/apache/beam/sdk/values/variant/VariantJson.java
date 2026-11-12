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

import static org.apache.beam.sdk.values.variant.VariantUtil.MAX_DECIMAL_PRECISION;
import static org.apache.beam.sdk.values.variant.VariantUtil.checkNestingDepth;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/** Converts between JSON text and {@link Variant}, following Spark's parse_json and to_json. */
final class VariantJson {
  private VariantJson() {}

  /**
   * Jackson's defaults already bound nesting depth, string length and number length, so a hostile
   * document fails fast instead of exhausting memory or the stack.
   */
  private static final JsonFactory JSON_FACTORY = new JsonFactory();

  static Variant parse(String json) {
    try (JsonParser parser = JSON_FACTORY.createParser(json)) {
      JsonToken first = parser.nextToken();
      if (first == null) {
        throw new IllegalArgumentException("Cannot parse an empty string as JSON");
      }
      VariantEncoder encoder = new VariantEncoder();
      appendValue(parser, first, encoder, 0);
      if (parser.nextToken() != null) {
        throw new IllegalArgumentException("Unexpected content after the end of the JSON value");
      }
      return encoder.build();
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Invalid JSON: " + e.getOriginalMessage(), e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void appendValue(
      JsonParser parser, JsonToken token, VariantEncoder encoder, int depth) throws IOException {
    switch (token) {
      case START_OBJECT:
        {
          checkNestingDepth(depth);
          int start = encoder.position();
          List<VariantEncoder.FieldEntry> fields = new ArrayList<>();
          JsonToken next;
          while ((next = parser.nextToken()) == JsonToken.FIELD_NAME) {
            int id = encoder.addKey(parser.currentName());
            fields.add(new VariantEncoder.FieldEntry(id, encoder.position() - start));
            appendValue(parser, nextValueToken(parser), encoder, depth + 1);
          }
          if (next != JsonToken.END_OBJECT) {
            throw new IllegalArgumentException("Malformed JSON object");
          }
          encoder.finishObject(start, fields);
          break;
        }
      case START_ARRAY:
        {
          checkNestingDepth(depth);
          int start = encoder.position();
          List<Integer> offsets = new ArrayList<>();
          JsonToken next;
          while ((next = nextValueToken(parser)) != JsonToken.END_ARRAY) {
            offsets.add(encoder.position() - start);
            appendValue(parser, next, encoder, depth + 1);
          }
          encoder.finishArray(start, offsets);
          break;
        }
      case VALUE_STRING:
        encoder.appendString(parser.getText());
        break;
      case VALUE_NUMBER_INT:
        if (parser.getNumberType() == JsonParser.NumberType.BIG_INTEGER) {
          appendBigInteger(parser.getBigIntegerValue(), encoder);
        } else {
          encoder.appendLong(parser.getLongValue());
        }
        break;
      case VALUE_NUMBER_FLOAT:
        appendFloatingPoint(parser, encoder);
        break;
      case VALUE_TRUE:
        encoder.appendBoolean(true);
        break;
      case VALUE_FALSE:
        encoder.appendBoolean(false);
        break;
      case VALUE_NULL:
        encoder.appendNull();
        break;
      default:
        throw new IllegalArgumentException("Unexpected JSON token: " + token);
    }
  }

  private static JsonToken nextValueToken(JsonParser parser) throws IOException {
    JsonToken token = parser.nextToken();
    if (token == null) {
      throw new IllegalArgumentException("Unexpected end of JSON input");
    }
    return token;
  }

  private static void appendBigInteger(BigInteger value, VariantEncoder encoder) {
    if (value.abs().toString().length() <= MAX_DECIMAL_PRECISION) {
      encoder.appendDecimal(new BigDecimal(value));
    } else {
      encoder.appendDouble(value.doubleValue());
    }
  }

  /**
   * Numbers without an exponent are kept as exact decimals when they fit, so {@code 19.99} stays
   * {@code 19.99}. Numbers with an exponent, or too many digits, become doubles.
   */
  private static void appendFloatingPoint(JsonParser parser, VariantEncoder encoder)
      throws IOException {
    String text = parser.getText();
    if (text.indexOf('e') < 0 && text.indexOf('E') < 0) {
      BigDecimal decimal = new BigDecimal(text);
      if (decimal.precision() <= MAX_DECIMAL_PRECISION
          && decimal.scale() >= 0
          && decimal.scale() <= MAX_DECIMAL_PRECISION) {
        encoder.appendDecimal(decimal);
        return;
      }
    }
    encoder.appendDouble(parser.getDoubleValue());
  }

  static String toJson(Variant variant) {
    StringWriter writer = new StringWriter();
    try (JsonGenerator generator = JSON_FACTORY.createGenerator(writer)) {
      write(variant, generator, 0);
    } catch (JsonProcessingException e) {
      // Writing to a StringWriter cannot fail with real I/O errors, only Jackson's own limits.
      throw new IllegalArgumentException(
          "Cannot convert variant to JSON: " + e.getOriginalMessage(), e);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return writer.toString();
  }

  private static void write(Variant variant, JsonGenerator generator, int depth)
      throws IOException {
    switch (variant.getType()) {
      case OBJECT:
        checkNestingDepth(depth);
        generator.writeStartObject();
        for (int i = 0; i < variant.numFields(); i++) {
          generator.writeFieldName(variant.fieldNameAt(i));
          write(variant.fieldValueAt(i), generator, depth + 1);
        }
        generator.writeEndObject();
        break;
      case ARRAY:
        checkNestingDepth(depth);
        generator.writeStartArray();
        for (int i = 0; i < variant.numElements(); i++) {
          write(variant.getElement(i), generator, depth + 1);
        }
        generator.writeEndArray();
        break;
      case NULL:
        generator.writeNull();
        break;
      case BOOLEAN:
        generator.writeBoolean(variant.getBoolean());
        break;
      case INT8:
      case INT16:
      case INT32:
      case INT64:
        generator.writeNumber(variant.getLong());
        break;
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        generator.writeNumber(variant.getDecimal().toPlainString());
        break;
      case FLOAT:
        generator.writeNumber(variant.getFloat());
        break;
      case DOUBLE:
        generator.writeNumber(variant.getDouble());
        break;
      case STRING:
        generator.writeString(variant.getString());
        break;
      case BINARY:
        generator.writeString(Base64.getEncoder().encodeToString(variant.getBinary()));
        break;
      case DATE:
        generator.writeString(DateTimeFormatter.ISO_LOCAL_DATE.format(variant.getDate()));
        break;
      case TIME:
        generator.writeString(DateTimeFormatter.ISO_LOCAL_TIME.format(variant.getTime()));
        break;
      case TIMESTAMP:
      case TIMESTAMP_NANOS:
        generator.writeString(DateTimeFormatter.ISO_INSTANT.format(variant.getTimestamp()));
        break;
      case TIMESTAMP_NTZ:
      case TIMESTAMP_NTZ_NANOS:
        generator.writeString(
            DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(variant.getTimestampNtz()));
        break;
      case UUID:
        generator.writeString(variant.getUuid().toString());
        break;
      default:
        throw new IllegalStateException("Unexpected variant type: " + variant.getType());
    }
  }
}
