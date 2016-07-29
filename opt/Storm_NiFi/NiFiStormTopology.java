package NiFi;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.storm.NiFiDataPacket;
import org.apache.nifi.storm.NiFiSpout;

public class NiFiStormTopology {

public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {

final String Path = args[0];
// Build a Site-To-Site client config
SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
.url("http://localhost:8099/nifi")
.portName("OUT")
.buildConfig();
// Build a topology starting with a NiFiSpout
TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("nifi", new NiFiSpout(clientConfig));
// Add a bolt that prints the attributes and content
builder.setBolt("print", new BaseBasicBolt() {

private static final long serialVersionUID = 1L;

public void execute(Tuple tuple, BasicOutputCollector collector) {
NiFiDataPacket dp = (NiFiDataPacket) tuple.getValueByField("nifiDataPacket");

//Print Attributes
System.out.println("Attributes: " + dp.getAttributes());

//Print Content
System.out.println("Content: " + new String(dp.getContent()));
//Save it to local
try {
	String fileExt = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
	FileUtils.writeStringToFile(new File(Path+fileExt), new String(dp.getContent()));
	} 
catch (IOException e) {
	e.printStackTrace();
}

}

public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}).shuffleGrouping("nifi");
// Submit the topology running in Cluster mode
Config conf = new Config();
StormSubmitter.submitTopology("NiFi-Storm", conf, builder.createTopology());

/*
Comment above line and uncomment below two lines to run in local mode
LocalCluster cluster = new LocalCluster();
cluster.submitTopology("test", conf, builder.createTopology());

*/
}
}