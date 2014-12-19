package com.ugam.crawler.core.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;

public class StormKafkaConsumer implements IRichSpout {

	private static final Logger LOG = LoggerFactory.getLogger(StormKafkaConsumer.class);
	private static final long serialVersionUID = 1572075547617400825L;
	private ConsumerConnector _consumer;
	private String zookeeper;
	private String groupId;
	private String topic;
	private int _numPartition;
	private transient SpoutOutputCollector _collector;
	private final Scheme _serializationScheme;
	private List<KafkaStream<byte[], byte[]>> _streams;
	private ConsumerIterator<byte[], byte[]> _iterator;
	private String _serviceUrl;
	
	public StormKafkaConsumer(String zookeeper, String groupId, String topic, int numPartition, String serviceUrl) {
		super();
		this.zookeeper = zookeeper;
		this.groupId = groupId;
		this.topic = topic;
		this._numPartition = numPartition;
		this._serviceUrl = serviceUrl;
		_serializationScheme = new RawScheme();
	}
	
	private void initializeConsumerAndStream() {
		ConsumerConfig consumerConfig = createConsumerConfig();
		_consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = _consumer.createMessageStreams(topicCountMap);
		_streams = consumerMap.get(topic);
	}

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		initializeConsumerAndStream();
		_collector = collector;
	}

	@Override
	public void nextTuple() {
		if(_iterator == null) {
			_iterator = _streams.get(0).iterator();
		}
		int size = 0;
		while (size < 100 && _iterator.hasNext()) {
        	final MessageAndMetadata<byte[], byte[]> message = _iterator.next();
            final KafkaMessageId id = new KafkaMessageId(message.partition(), message.offset());
            size++;
            _collector.emit(_serializationScheme.deserialize(message.message()), id);
        }
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(_serializationScheme.getOutputFields());
	}
	
	@Override
	public void ack(Object msgId) {
		if (msgId instanceof KafkaMessageId) {
            final KafkaMessageId id = (KafkaMessageId) msgId;
            LOG.info("kafka message {} acknowledged", id);
            _consumer.commitOffsets();
		}
	}
	
	@Override
	public void deactivate() {
	}
	
	@Override
	public void activate() {
	}
	
	@Override
	public void close() {
		if(_consumer != null) {
			_consumer.shutdown();
		}
	}

	@Override
	public void fail(Object msgId) {
		if (msgId instanceof KafkaMessageId) {
			final KafkaMessageId id = (KafkaMessageId) msgId;
            LOG.info("kafka message {} failed", id);
		}	
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "20000");
        //props.put("auto.commit.interval.ms", "10000");
        return new ConsumerConfig(props);
    }

}
