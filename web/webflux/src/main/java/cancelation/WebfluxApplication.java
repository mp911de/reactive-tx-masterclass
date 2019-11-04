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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.embedded.netty.NettyServerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Mark Paluch
 */
@SpringBootApplication
@EnableR2dbcRepositories(considerNestedRepositories = true)
public class WebfluxApplication {

	public static void main(String[] args) {
		SpringApplication.run(WebfluxApplication.class, args);
	}

	// @Bean
	NettyServerCustomizer customizer() {
		return httpServer -> {
			return httpServer.tcpConfiguration(tcpServer -> {

				return tcpServer.doOnConnection(connection -> {

					connection.addHandlerLast(new ChannelDuplexHandler() {

						@Override
						public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
							super.channelReadComplete(ctx);
							ctx.channel().config().setAutoRead(true);
						}
					});
				});
			});
		};
	}

	@RestController
	static class WebController {

		final TransactionalService transactionalService;

		WebController(TransactionalService transactionalService) {
			this.transactionalService = transactionalService;
		}

		@PostMapping
		Flux<Integer> longRunningSave() {

			Flux<Integer> counter = Flux.interval(Duration.ZERO, Duration.ofSeconds(10)) //
					.take(6) //
					.map(Long::intValue);

			return transactionalService.save(counter).doOnCancel(() -> System.out.println("Canceled!"));
		}

		@GetMapping
		Flux<Event> findAll() {
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
		public Flux<Integer> save(Flux<Integer> counter) {

			return counter.flatMap(i -> {

				int from = 10 * i;
				System.out.printf("Round %d, from %d to %d%n", i, from, from + 10);

				return Flux.range(from, 10).flatMap(item -> eventRepository.save(new Event(item))).then(Mono.just(i));
			});
		}

		public Flux<Event> findAll() {
			return eventRepository.findAll();
		}

		public void deleteAll() {
			eventRepository.deleteAll();
		}
	}

	interface EventRepository extends ReactiveCrudRepository<Event, Integer> {

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
