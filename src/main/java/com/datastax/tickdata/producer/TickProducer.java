package com.datastax.tickdata.producer;

import java.util.List;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.tickdata.DataLoader;
import com.datastax.tickdata.producer.TickGenerator.TickValue;

public class TickProducer {

	public TickProducer() {
		long events = Long.parseLong(PropertyHelper.getProperty("noOfTicks", "1000000"));
		Random rnd = new Random();

		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		DataLoader dataLoader = new DataLoader();
		List<String> exchangeSymbols = dataLoader.getExchangeData();

		TickGenerator tickGenerator = new TickGenerator(exchangeSymbols);

		for (long nEvents = 0; nEvents < events; nEvents++) {

			TickValue tickValueRandom = tickGenerator.getTickValueRandom();
			String str = tickValueRandom.tickSymbol + "#" + tickValueRandom.value;
			KeyedMessage<String, String> data = new KeyedMessage<String, String>("tick_stream", null,str);				
			producer.send(data);
		}
		producer.close();

	}

	public static void main(String args[]) {
		new TickProducer();
	}
}