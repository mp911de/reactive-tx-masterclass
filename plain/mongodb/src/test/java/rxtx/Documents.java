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

import java.util.StringJoiner;

import org.bson.Document;

/**
 * Utility to print a {@link Documents} to {@link System#out}
 */
final class Documents {

	public static void print(Document document) {

		int width = document.size() * 20;

		System.out.println(String.format("%0" + width + "d", 0).replace("0", "="));

		StringJoiner joiner = new StringJoiner("|");
		document.forEach((s, o) -> {
			joiner.add(String.format("%-20s", o));
		});

		System.out.println("         " + joiner);

		System.out.println(String.format("%0" + width + "d", 0).replace("0", "="));
		System.out.println();
	}
}
