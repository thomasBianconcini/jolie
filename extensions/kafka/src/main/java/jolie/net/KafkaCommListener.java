package jolie.net;

import jolie.Interpreter;
import jolie.net.ext.CommProtocolFactory;
import jolie.net.ports.InputPort;
import jolie.net.protocols.CommProtocol;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaCommListener extends CommListener {
	private final String kafkaTopicName;
	private final String bootstrapServers;
	private final String id;
	private final Properties prop = new Properties();
	final CommProtocol protocol;
	private final KafkaCommChannel kafkaCommChannel;
	KafkaConsumer< String, byte[] > consumer;
	private boolean keepRun = true;

	public KafkaCommListener( Interpreter interpreter, CommProtocolFactory protocolFactory, InputPort inputPort )
		throws IOException {
		super( interpreter, protocolFactory, inputPort );
		kafkaTopicName = locationAttributes().get( "topic" );
		bootstrapServers = locationAttributes().get( "bootstrap" );
		id = locationAttributes().get( "id" );
		prop.put( "bootstrap.servers", bootstrapServers );
		prop.put( "group.id", id );
		prop.put( "auto.commit.interval.ms", "1000" );
		prop.put( "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer" );
		prop.put( "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer" );
		this.protocol = createProtocol();
		// Creation of Kafka Cosumer
		this.consumer = new KafkaConsumer<>( prop );
		consumer.subscribe( Arrays.asList( kafkaTopicName ) );
		kafkaCommChannel = new KafkaCommChannel( inputPort.location(), protocol );
		kafkaCommChannel.setParentInputPort( inputPort );
	}

	@Override
	public void run() {
		while( keepRun ) {
			ConsumerRecords< String, byte[] > records = consumer.poll( Duration.ofSeconds( 1 ) );
			// wait until a message in the topic
			while( records.isEmpty() ) {
				records = consumer.poll( Duration.ofSeconds( 1 ) );
			}
			for( ConsumerRecord< String, byte[] > record : records ) {
				// for each message in the topic
				byte[] byteToSend = record.value();
				KafkaMessage msg = new KafkaMessage( byteToSend );
				kafkaCommChannel.setData( msg );
				interpreter().commCore().scheduleReceive( kafkaCommChannel, inputPort() );
			}
		}
	}

	// return the map of the attributes
	public Map< String, String > locationAttributes() throws IOException {
		return KafkaConnectionHandler.getConnection( inputPort().location() ).getLocationAttributes();
	}

	@Override
	public void shutdown() {
		try {
			// Close current connection.
			keepRun = false;
			consumer.close();
			KafkaConnectionHandler.closeConnection( inputPort().location() );
		} catch( IOException ex ) {
			Logger.getLogger( KafkaConnectionHandler.class.getName() ).log( Level.WARNING, null, ex );
		}
	}
}
