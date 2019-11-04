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
package cancelation;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.repository.config.EnableJdbcRepositories;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Mark Paluch
 */
@SpringBootApplication
@EnableJdbcRepositories(considerNestedRepositories = true)
public class ServletApplication {

	public static void main(String[] args) {
		SpringApplication.run(ServletApplication.class, args);
	}

	@RestController
	static class WebController {

		final TransactionalService transactionalService;

		WebController(TransactionalService transactionalService) {
			this.transactionalService = transactionalService;
		}

		@PostMapping
		void longRunningSave() throws InterruptedException {

			for (int i = 0; i < 6; i++) {

				int from = 10 * i;
				int to = from + 10;
				System.out.printf("Round %d, from %d to %d%n", i, from, to);

				List<Integer> items = IntStream.range(from, to).boxed().collect(Collectors.toList());
				transactionalService.save(items);

				try {
					TimeUnit.SECONDS.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
					throw e;
				}
			}
		}

		@GetMapping
		List<Event> findAll() {
			return transactionalService.findAll();
		}
	}

	@Component
	public static class TransactionalService {

		private final EventRepository eventRepository;

		public TransactionalService(EventRepository eventRepository) {
			this.eventRepository = eventRepository;
		}

		@Transactional
		public void save(List<Integer> items) {
			items.forEach(item -> eventRepository.save(new Event(item)));
		}

		public List<Event> findAll() {
			return eventRepository.findAll();
		}

		public void deleteAll() {
			eventRepository.deleteAll();
		}
	}

	interface EventRepository extends CrudRepository<Event, Integer> {

		@Override
		List<Event> findAll();
	}

	static class Event {

		@Id Integer id;

		int counter;

		public Event(int counter) {
			this.counter = counter;
		}

		public Integer getId() {
			return this.id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public int getCounter() {
			return this.counter;
		}

		public void setCounter(int counter) {
			this.counter = counter;
		}
	}
}
