package NiFi;



import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.storm.NiFiDataPacket;
import org.apache.nifi.storm.NiFiSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class NiFiStormTopology {

public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException, SQLException {
/*
if (!(args.length == 1)) {
	    System.err.println("###################################################################################################");
	    System.err.println("#### Usage: storm jar NiFiStormTopology.jar NiFi.NiFiStormTopology </LocalPath/to/save/output> ####");
	    System.err.println("###################################################################################################");
	    System.exit(2);
	  }
*/
//final String Path = args[0];

// Build a Site-To-Site client config
//SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
//.url("http://localhost:9090/nifi")
//.portName("OUTS")
//.buildConfig();

SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
.url("http://ec2-35-163-103-150.us-west-2.compute.amazonaws.com:9090/nifi")
.portName("OUT")
.buildConfig();

System.setProperty("hadoop.home.dir", "/");
// Build a topology starting with a NiFiSpout
TopologyBuilder builder = new TopologyBuilder();
builder.setSpout("nifi", new NiFiSpout(clientConfig));
// Add a bolt that prints the attributes and content
builder.setBolt("print", new BaseBasicBolt() {

private static final long serialVersionUID = 1L;

// Below 2 Lines are for Phoenix Connectivity
PreparedStatement updateStatement = null;
//Statement stmt = null;
Connection con = DriverManager.getConnection("jdbc:phoenix:ec2-35-163-103-150.us-west-2.compute.amazonaws.com:9091:/hbase-unsecure");

public void execute(Tuple tuple, BasicOutputCollector collector) {
NiFiDataPacket dp = (NiFiDataPacket) tuple.getValueByField("nifiDataPacket");

//Print Attributes
System.err.println("###################################################################################################");
System.out.println("UUID: " + dp.getAttributes().get("uuid"));
System.out.println("BULLETIN_LEVEL: " + dp.getAttributes().get("BULLETIN_LEVEL"));
System.out.println("EVENT_DATE: " + dp.getAttributes().get("EVENT_DATE"));
System.out.println("EVENT_TYPE: " + dp.getAttributes().get("EVENT_TYPE"));
//Print Content
System.out.println("Content: " + new String(dp.getContent()));
System.err.println("###################################################################################################");

//Save it to local
try {
	//String fileExt = new SimpleDateFormat("yyyyMMddhhmmss").format(new Date());
	//FileUtils.writeStringToFile(new File(uuid), new String(dp.getContent()));

	String UUID = (String) dp.getAttributes().get("uuid");
	String BULLETIN_LEVEL = (String) dp.getAttributes().get("BULLETIN_LEVEL");
	String EVENT_DATE = (String) dp.getAttributes().get("EVENT_DATE");
	String EVENT_TYPE = (String) dp.getAttributes().get("EVENT_TYPE");
	String CONTENT = new String(dp.getContent());

	// Below 4 Lines are for Phoenix Upset
	updateStatement = con.prepareStatement("upsert into NIFI_LOG(UUID, BULLETIN_LEVEL, EVENT_DATE, EVENT_TYPE, CONTENT) values(?, ?, ?, ?, ?)");
	setParameters(updateStatement, UUID, BULLETIN_LEVEL, EVENT_DATE, EVENT_TYPE, CONTENT);
	updateStatement.executeUpdate();
	//stmt = con.createStatement();
    //stmt.executeUpdate("upsert into NIFI_LOG values ("+UUID+","+BULLETIN_LEVEL+","+EVENT_DATE+","+EVENT_TYPE+","+CONTENT+")");
    con.commit();

	} 
catch (SQLException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
} 

}

public void declareOutputFields(OutputFieldsDeclarer declarer) {}


}).shuffleGrouping("nifi");
// Submit the topology running in Cluster mode
Config conf = new Config();
StormSubmitter.submitTopology("NiFi-Storm", conf, builder.createTopology());


//Comment above line and uncomment below two lines to run in local mode
//LocalCluster cluster = new LocalCluster();
//cluster.submitTopology("test", conf, builder.createTopology());

}
private static void setParameters(PreparedStatement updateStatement, String UUID, String BULLETIN_LEVEL, String EVENT_DATE, String EVENT_TYPE, String CONTENT)
		throws SQLException {
	updateStatement.setString(1, UUID);
	updateStatement.setString(2, BULLETIN_LEVEL);
	updateStatement.setString(3, EVENT_DATE);
	updateStatement.setString(4, EVENT_TYPE);
	updateStatement.setString(5, CONTENT);
}	
}