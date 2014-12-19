package com.ugam.crawler.core.topology;

import java.util.Map;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;

import com.ugam.crawler.core.ConfigPropertyProvider;
import com.ugam.crawler.core.consumer.StormKafkaConsumer;

public abstract class PlatformTopology {

    private static final Logger LOG = LoggerFactory.getLogger(PlatformTopology.class);

    private TopologyBuilder builder;

    private String zkHost;

    private String brokerList;

    private String localMode = "local";
    
    private Map<String, String> configProperties;
    
    private String url;

    public PlatformTopology(String[] args) {
        CommandLine cmd = getCommandLineOptions(args);
        if (cmd == null) {
            System.exit(-1);
        }
        
        url = cmd.getOptionValue("url");
        configProperties = new ConfigPropertyProvider(url).getConfigProperties(getTopologyName());
        this.zkHost = configProperties.get("zookeeper");
        this.brokerList = configProperties.get("brokerList");
        
        
        if (cmd.hasOption("cluster")) {
            localMode = "cluster";
        }
        LOG.info("Creating Topology {} : {}", this.getClass().getName(), this);
    }

    public void create() {
        builder = build(zkHost, brokerList);
        submitToCluster();
    }
    
    public abstract TopologyBuilder build(String zkHost, String brokerList);

    public abstract String getTopologyName();

    public void submitToCluster() {
        Config config = new Config();
        config.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);
        config.putAll(configProperties);
        config.put(Config.TOPOLOGY_WORKERS, numberOfWorkers());
        try {
            if ("local".equals(localMode)) {
                LocalCluster localCluster = new LocalCluster();
                localCluster.submitTopology(getTopologyName(), config, builder.createTopology());
            } else {
                StormSubmitter.submitTopology(getTopologyName(), config, builder.createTopology());
            }

        } catch (Exception e) {
            LOG.error("Unable to submit topology to cluster", e);
        }
    }
    
    public String getZooKeeperHost() {
        return zkHost;
    }

    public String getBrokerList() {
        return brokerList;
    }
    
    protected void setHighLevelKafkaSpout(TopologyBuilder builder, String zookeeper, String topicName, int numPartitions, String spoutId) {
    	String groupId = getTopologyName() + "_" + spoutId;
    	
    	/*String serviceUrl = url + "/consumer/stream/";
		try {
			MultivaluedMap<String, String> paramMap = new MultivaluedMapImpl();
			paramMap.add("group_id", groupId);
			paramMap.add("num_partition", "" + numPartitions);
			new ClientConnection().executeWithParam(serviceUrl, paramMap, HttpMethod.POST);
		} catch (Exception e) {
			LOG.error("Error Occured In Calling WebService ", e);
		}*/
		
    	StormKafkaConsumer stormKafkaConsumer = new StormKafkaConsumer(zookeeper, groupId, topicName, 3, url);
    	builder.setSpout(spoutId, stormKafkaConsumer, numPartitions).setNumTasks(numPartitions);
	}
    
    protected void setSimpleKafkaSpout(TopologyBuilder builder, String topicName, String spoutId, String zkPath, String zkHost, int task) {
    	BrokerHosts brokerHosts = new ZkHosts(zkHost);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topicName, zkPath, spoutId);
        spoutConfig.bufferSizeBytes = 504857600;
        spoutConfig.fetchSizeBytes = 504857600;
        spoutConfig.maxOffsetBehind = Long.MAX_VALUE;
        
        KafkaSpout spout = new KafkaSpout(spoutConfig);
        builder.setSpout(spoutId, spout, task).setNumTasks(task);
	}

    private CommandLine getCommandLineOptions(String[] args) {
        Options options = new Options();

        Option urlOption =
            new Option("url", true, "The url string in the form http://<server-ip>:<port>/");
        urlOption.setRequired(true);
        options.addOption(urlOption);

        Option clusterModeOption =
            new Option("cluster", false, "Use it to declare if the topology needs to be deployed to the cluster");
        options.addOption(clusterModeOption);

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(e.getMessage(), options);
        }
        return cmd;
    }

    @Override
    public String toString() {
        return "PlatformTopology [zkHost=" + zkHost + ", brokerList=" + brokerList + ", localMode=" + localMode + "]";
    }

    public int getTimeout() {
        return -1;
    }

    public int getTickTupleFrequency() {
        return -1;
    }
    
    public int numberOfWorkers() {
    	return Integer.parseInt(configProperties.get("workers") == null ? "1" : configProperties.get("workers"));
    }
    
    public int numberOfTask(String key) {
    	return configProperties.get(key) == null ? 1 : Integer.parseInt(configProperties.get(key));
    }

}
