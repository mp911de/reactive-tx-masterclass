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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringJoiner;

/**
 * Utility to print a result set to {@link System#out}
 */
public final class Rows {

	public static void print(ResultSet rs) throws SQLException {

		int width = rs.getMetaData().getColumnCount() * 20;

		System.out.println(String.format("%0" + width + "d", 0).replace("0", "="));

		StringJoiner joiner = new StringJoiner("|");
		for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
			joiner.add(String.format("%-20s", rs.getMetaData().getColumnLabel(i + 1)));
		}

		System.out.println("Columns: " + joiner);

		boolean foundRow = false;

		while (rs.next()) {

			joiner = new StringJoiner("|");
			for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
				joiner.add(String.format("%-20s", rs.getString(i + 1)));
			}
			foundRow = true;

			System.out.println("         " + joiner);
		}

		if (!foundRow) {
			System.out.println("No data");
		}
		System.out.println(String.format("%0" + width + "d", 0).replace("0", "="));
		System.out.println();
	}
}
