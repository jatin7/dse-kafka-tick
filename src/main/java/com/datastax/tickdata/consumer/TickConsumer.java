package com.datastax.tickdata.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.mortbay.log.Log;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.tickdata.model.TickData;

public class TickConsumer {

	private BlockingQueue<List<TickData>> queueTickData;
	private final ConsumerConnector consumer;
	private String topic;
	private long TOTAL_COUNT = 0;

	public TickConsumer(BlockingQueue<List<TickData>> queueTickData) {
		this.queueTickData = queueTickData;

		ConsumerConfig consumerConfig = createConsumerConfig(PropertyHelper.getProperty("zk", "localhost:2181"),
				PropertyHelper.getProperty("consumerGroup", "test-group"));
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

		this.topic = "tick_stream";

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1);

		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap,
				new kafka.serializer.StringDecoder(null), new kafka.serializer.StringDecoder(null));
		List<KafkaStream<String, String>> streams = consumerMap.get(topic);

		KafkaStream<String, String> stream = streams.get(0);

		Executors.newSingleThreadExecutor().execute(new ConsumerThread(stream));
	}

	public class ConsumerThread implements Runnable {
		private KafkaStream<String, String> stream;

		public ConsumerThread(KafkaStream<String, String> stream) {
			this.stream = stream;
		}

		public void run() {
			ConsumerIterator<String, String> it = stream.iterator();
			while (it.hasNext()) {
				String message = it.next().message();

				TickData tickData = new TickData(message.substring(0, message.indexOf("#")), Double.parseDouble(message
						.substring(message.indexOf("#") + 1)));
				ArrayList<TickData> list = new ArrayList<TickData>(1);

				list.add(tickData);

				if (list.size() > 100) {
					try {
						queueTickData.put(new ArrayList<TickData>(list));
						list.clear();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				Log.debug("Got Message " + message);
				TOTAL_COUNT++;
			}
		}
	}

	private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);
	}

	public String getTicksGenerated() {
		return TOTAL_COUNT + "";
	}
}
