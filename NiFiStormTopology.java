package wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.storm.NiFiDataPacket;
import org.apache.nifi.storm.NiFiSpout;
public class NiFiStormTopology {
public static void main( String[] args ) {
// Build a Site-To-Site client config
SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
.url("http://HW12626:8080/nifi")
.portName("OUT")
.buildConfig();
// Build a topology starting with a NiFiSpout
TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("nifi", new NiFiSpout(clientConfig));
// Add a bolt that prints the attributes and content
builder.setBolt("print", new BaseBasicBolt() {

private static final long serialVersionUID = 1L;
@Override
public void execute(Tuple tuple, BasicOutputCollector collector) {
NiFiDataPacket dp = (NiFiDataPacket) tuple.getValueByField("nifiDataPacket");
System.out.println("Attributes: " + dp.getAttributes());
System.out.println("Content: " + new String(dp.getContent()));
}
@Override
public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}).shuffleGrouping("nifi");
// Submit the topology running in local mode
Config conf = new Config();
LocalCluster cluster = new LocalCluster();
cluster.submitTopology("test", conf, builder.createTopology());
}
}
