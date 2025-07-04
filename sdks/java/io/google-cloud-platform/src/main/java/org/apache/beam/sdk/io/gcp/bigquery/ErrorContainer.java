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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.FailsafeValueInSingleWindow;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * ErrorContainer interface.
 *
 * @param <T>
 */
public interface ErrorContainer<T> extends Serializable {
  void add(
      List<ValueInSingleWindow<T>> failedInserts,
      TableDataInsertAllResponse.InsertErrors error,
      TableReference ref,
      FailsafeValueInSingleWindow<TableRow, TableRow> tableRow);

  ErrorContainer<TableRow> TABLE_ROW_ERROR_CONTAINER =
      (failedInserts, error, ref, tableRow) ->
          failedInserts.add(
              ValueInSingleWindow.of(
                  Preconditions.checkArgumentNotNull(tableRow.getFailsafeValue()),
                  tableRow.getTimestamp(),
                  tableRow.getWindow(),
                  tableRow.getPaneInfo()));

  ErrorContainer<BigQueryInsertError> BIG_QUERY_INSERT_ERROR_ERROR_CONTAINER =
      (failedInserts, error, ref, tableRow) -> {
        BigQueryInsertError err = new BigQueryInsertError(tableRow.getFailsafeValue(), error, ref);
        failedInserts.add(
            ValueInSingleWindow.of(
                err, tableRow.getTimestamp(), tableRow.getWindow(), tableRow.getPaneInfo()));
      };
}
