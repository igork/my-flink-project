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
package myflink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.File;
import java.net.URL;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HotItems {

	public static void main(String[] args) throws Exception {


		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// Tell the system to handle according to EventTime
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// In order that the results printed to the console are not out of order, we configure the global concurrency to 1
		env.setParallelism(1);

		URL fileUrl = HotItems.class.getClassLoader().getResource("UserBehavior.csv");
		Path filePath = Path.fromLocalFile(new File(fileUrl.toURI()));
		PojoTypeInfo<UserBehavior> pojoType = (PojoTypeInfo<UserBehavior>) TypeExtractor.createTypeInfo(UserBehavior.class);
		String[] fieldOrder = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
		PojoCsvInputFormat<UserBehavior> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);


		env

			.createInput(csvInput, pojoType)
			// Extract time and generate watermark
			.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
				@Override
				public long extractAscendingTimestamp(UserBehavior userBehavior) {
					//Raw data is in seconds and converted to milliseconds
					return userBehavior.timestamp * 1000;
				}
			})
			// Filter out only clickable data
			.filter(new FilterFunction<UserBehavior>() {
				@Override
				public boolean filter(UserBehavior userBehavior) throws Exception {
					return userBehavior.behavior.equals("pv");
				}
			})
			.keyBy("itemId")
			.timeWindow(Time.minutes(60), Time.minutes(5))
			.aggregate(new CountAgg(), new WindowResultFunction())
			.keyBy("windowEnd")
			.process(new TopNHotItems(3))
			.print();

		env.execute("Hot Items Job");
	}

	/*
	Find the top N popular hits in a window, key is the window timestamp, and the output is TopN result string
	*/
	public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

		private final int topSize;

		public TopNHotItems(int topSize) {
			this.topSize = topSize;
		}

		// Used to store the status of products and clicks.
		// After receiving the data of the same window, the TopN calculation is triggered
		private ListState<ItemViewCount> itemState;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>(
				"itemState-state",
				ItemViewCount.class);
			itemState = getRuntimeContext().getListState(itemsStateDesc);
		}

		@Override
		public void processElement(
			ItemViewCount input,
			Context context,
			Collector<String> collector) throws Exception {

			//Each piece of data is saved to the state
			itemState.add(input);

			// Register the EventTime Timer of windowEnd + 1, when triggered,
			// it means that all the product data belonging to the windowEnd
			// window are collected
			context.timerService().registerEventTimeTimer(input.windowEnd + 1);
		}

		@Override
		public void onTimer(
			long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
			// Get all product clicks received
			List<ItemViewCount> allItems = new ArrayList<>();
			for (ItemViewCount item : itemState.get()) {
				allItems.add(item);
			}
			// Clear the data in the state in advance to free up space
			itemState.clear();
			// Sort by click
			allItems.sort(new Comparator<ItemViewCount>() {
				@Override
				public int compare(ItemViewCount o1, ItemViewCount o2) {
					return (int) (o2.viewCount - o1.viewCount);
				}
			});
			// Format ranking information as String for easy printing
			StringBuilder result = new StringBuilder();
			result.append("====================================\n");
			result.append("time: ").append(new Timestamp(timestamp-1)).append("\n");
                        for (int i=0; i<allItems.size() && i < topSize; i++) {
				ItemViewCount currentItem = allItems.get(i);
				// No1: Product ID = 12224 Views = 2413
				result.append("No").append(i).append(":")
					.append("  itemID=").append(currentItem.itemId)
					.append("  views=").append(currentItem.viewCount)
					.append("\n");
			}
			result.append("====================================\n\n");

			// Control output frequency and simulate real-time rolling results
			Thread.sleep(1000);

			out.collect(result.toString());
		}
	}

	/** Results for output window */
	public static class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

		@Override
		public void apply(
			Tuple key,  // The primary key of the window, which is itemId
			TimeWindow window,
			Iterable<Long> aggregateResult, // The result of the aggregate function, which is the count value
			Collector<ItemViewCount> collector  // Output type is ItemViewCount
		) throws Exception {
			Long itemId = ((Tuple1<Long>) key).f0;
			Long count = aggregateResult.iterator().next();
			collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
		}
	}

	/** COUNT statistics aggregation function is implemented, each occurrence of a record plus */
	public static class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

		@Override
		public Long createAccumulator() {
			return 0L;
		}

		@Override
		public Long add(UserBehavior userBehavior, Long acc) {
			return acc + 1;
		}

		@Override
		public Long getResult(Long acc) {
			return acc;
		}

		@Override
		public Long merge(Long acc1, Long acc2) {
			return acc1 + acc2;
		}
	}

	/**
	 Product clicks (output type of window operation)
	 */
	public static class ItemViewCount {
		public long itemId;     //
		public long windowEnd;  // Window end timestamp
		public long viewCount;  // Product traffic

		public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
			ItemViewCount result = new ItemViewCount();
			result.itemId = itemId;
			result.windowEnd = windowEnd;
			result.viewCount = viewCount;
			return result;
		}
	}

	/** User Behavior Data Structure **/
	public static class UserBehavior {
		public long userId;
		public long itemId;
		public int categoryId;      // Product category ID
		public String behavior;     // User behavior, including ("pv", "buy", "cart", "fav")
		public long timestamp;      // Timestamp when the action occurred, in seconds
	}
}
