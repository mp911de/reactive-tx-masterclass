/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rxtx;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.StringJoiner;
import java.util.function.Function;

/**
 * @author Mark Paluch
 */
public class RowPrinter implements Function<Result, Mono<Void>> {

	boolean first = true;
	int width = 20;

	@Override
	public Mono<Void> apply(Result result) {

		return Flux.from(result.map((row, rowMetadata) -> {

			printRow(row, rowMetadata);
			return rowMetadata;
		})).doOnComplete(() -> {

			if (first) {
				System.out.println("No data");
			} else {
				System.out.println(String.format("%0" + width + "d", 0).replace("0", "="));
			}
			System.out.println();

		}).then();

	}

	private void printRow(Row row, RowMetadata rowMetadata) {

		Collection<String> columnNames = rowMetadata.getColumnNames();

		if (first) {
			width = columnNames.size() * 20;
			System.out.println(String.format("%0" + width + "d", 0).replace("0", "="));

			StringJoiner joiner = new StringJoiner("|");
			for (String columnName : columnNames) {
				joiner.add(String.format("%-20s", columnName));
			}

			System.out.println("Columns: " + joiner);
			first = false;
		}

		StringJoiner joiner = new StringJoiner("|");

		for (String columnName : columnNames) {
			joiner.add(String.format("%-20s", row.get(columnName)));
		}

		System.out.println("         " + joiner);
	}
}
