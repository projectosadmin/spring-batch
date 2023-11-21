/*
 * Copyright 2006-2007 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.batch.repeat.support;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemReader;
import org.springframework.batch.repeat.ExitStatus;
import org.springframework.batch.repeat.RepeatCallback;
import org.springframework.batch.repeat.RepeatContext;
import org.springframework.batch.repeat.callback.ItemReaderRepeatCallback;
import org.springframework.batch.repeat.callback.NestedRepeatCallback;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import static org.junit.Assert.*;
/**
 * Test various approaches to chunking of a batch. Not really a unit test, but
 * it should be fast.
 * 
 * @author Dave Syer
 * 
 */
public class ChunkedRepeatTests extends AbstractTradeBatchTests {

	int count = 0;

	/**
	 * Chunking using a dedicated TerminationPolicy. Transactions would be laid
	 * on at the level of chunkTemplate.execute() or the surrounding callback.
	 * 
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testChunkedBatchWithTerminationPolicy() throws Exception {

		RepeatTemplate repeatTemplate = new RepeatTemplate();
		final RepeatCallback callback = new ItemReaderRepeatCallback(provider, processor);

		final RepeatTemplate chunkTemplate = new RepeatTemplate();
		// The policy is resettable so we only have to resolve this dependency
		// once
		chunkTemplate.setCompletionPolicy(new SimpleCompletionPolicy(2));

		ExitStatus result = repeatTemplate.iterate(new NestedRepeatCallback(chunkTemplate, callback) {

			public ExitStatus doInIteration(RepeatContext context) throws Exception {
				count++; // for test assertion
				return super.doInIteration(context);
			}

		});

		assertEquals(NUMBER_OF_ITEMS, processor.count);
		// The chunk executes 3 times because the last one
		// returns false. We terminate the main batch when
		// we encounter a partially empty chunk.
		assertEquals(3, count);
		assertFalse(result.isContinuable());

	}

	/**
	 * Chunking with an asynchronous taskExecutor in the chunks. Transactions
	 * have to be at the level of the business callback.
	 * 
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testAsynchronousChunkedBatchWithCompletionPolicy() throws Exception {

		RepeatTemplate repeatTemplate = new RepeatTemplate();
		final RepeatCallback callback = new ItemReaderRepeatCallback(provider, processor);

		final TaskExecutorRepeatTemplate chunkTemplate = new TaskExecutorRepeatTemplate();
		// The policy is resettable so we only have to resolve this dependency
		// once
		chunkTemplate.setCompletionPolicy(new SimpleCompletionPolicy(2));
		chunkTemplate.setTaskExecutor(new SimpleAsyncTaskExecutor());

		ExitStatus result = repeatTemplate.iterate(new NestedRepeatCallback(chunkTemplate, callback) {

			public ExitStatus doInIteration(RepeatContext context) throws Exception {
				count++; // for test assertion
				return super.doInIteration(context);
			}

		});

		assertEquals(NUMBER_OF_ITEMS, processor.count);
		assertFalse(result.isContinuable());
		assertTrue("Expected at least 3 chunks but found: "+count, count>=3);

	}

	/**
	 * Explicit chunking of input data. Transactions would be laid on at the
	 * level of template.execute().
	 * 
	 * @throws Exception Exception
	 */
	@org.junit.Test
public void testChunksWithTruncatedItemProvider() throws Exception {

		RepeatTemplate template = new RepeatTemplate();

		// This pattern would work with an asynchronous callback as well
		// (but non-transactional in that case).

		class Chunker {
			boolean ready = false;

			int count = 0;

			void set() {
				ready = true;
			}

			boolean ready() {
				return ready;
			}

			boolean first() {
				return count == 0;
			}

			void reset() {
				count = 0;
				ready = false;
			}

			void increment() {
				count++;
			}
		}
		;

		final Chunker chunker = new Chunker();

		while (!chunker.ready()) {

			ItemReader truncated = new AbstractItemReader() {
				int count = 0;

				public Object read() throws Exception {
					if (count++ < 2)
						return provider.read();
					return null;
				}
			};
			chunker.reset();
			template.iterate(new ItemReaderRepeatCallback(truncated, processor) {

				public ExitStatus doInIteration(RepeatContext context) throws Exception {
					ExitStatus result = super.doInIteration(context);
					if (!result.isContinuable() && chunker.first()) {
						chunker.set();
					}
					chunker.increment();
					return result;
				}

			});

		}

		assertEquals(NUMBER_OF_ITEMS, processor.count);

	}

}
