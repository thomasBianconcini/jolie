package jolie.net;

import jolie.net.ports.OutputPort;
import jolie.net.protocols.CommProtocol;
import jolie.runtime.Value;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

public class KafkaCommChannel extends StreamingCommChannel {

	// General
	private final URI location;
	// Input
	private KafkaMessage data;
	//Output
	private CommMessage message = null;
	private Properties prop ;

	String kafka_topic_name;
	String bootstrap_servers;
	public KafkaCommChannel( URI location, CommProtocol protocol ) throws IOException {
		super(location,protocol);
		this.location=location;
		// add parametri
		kafka_topic_name = locationAttributes().get("topic");
		bootstrap_servers = locationAttributes().get("bootstrap");
		// valutare altre proprietà da aggiungere
	setToBeClosed( false );
	}
	@Override
	protected void closeImpl() throws IOException{
		KafkaConnectionHandler.closeConnection( location );
	}

	@Override protected CommMessage recvImpl() throws IOException {
		ByteArrayOutputStream ostream = new ByteArrayOutputStream();
		CommMessage returnMessage;
		//if we are an Input Port
		if(data != null)
		{
			returnMessage = protocol().recv( new ByteArrayInputStream( data.body ), ostream );
			return returnMessage;
		}
		//if we are an Outputport

		throw new IOException( "Wrong context for receive!" );
	}


	@Override protected void sendImpl( CommMessage message ) throws IOException {
		ByteArrayOutputStream ostream = new ByteArrayOutputStream();
		protocol().send( ostream, message, null );
		this.message=message;

		// OutputPort
		if(parentPort() instanceof OutputPort )
		{
			prop= new Properties();
			prop.put("bootstrap.servers",bootstrap_servers);
			prop.setProperty("kafka.topic.name",kafka_topic_name);
			KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(this.prop,new StringSerializer(),new ByteArraySerializer());
			ProducerRecord<String,byte[]>  record = new ProducerRecord<String ,byte[]>(prop.getProperty("kafka.topic.name"), ostream.toByteArray());
			producer.send((record));
			producer.close();

		}else {
			throw new IOException( "Port is of unexpected type!" );
		}
		// Input Port to cambe back to sender

	}

	public Map<String,String> locationAttributes()throws IOException{
		return KafkaConnectionHandler.getConnection( location ).getLocationAttributes();
	}
	public void setData (KafkaMessage msg)
	{
		this.data=data;
	}

}
