package jolie.net;

import jolie.Interpreter;
import jolie.net.ext.CommProtocolFactory;
import jolie.net.ports.InputPort;
import jolie.net.protocols.CommProtocol;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;


public class KafkaCommListener extends CommListener {
	private final Object lock = new Object();
	private final String kafkaTopicName;
	private final String bootstrapServers;
	private final Properties prop = new Properties();

	public KafkaCommListener( Interpreter interpreter, CommProtocolFactory protocolFactory, InputPort inputPort )
		throws IOException {
		super( interpreter, protocolFactory, inputPort );
		kafkaTopicName = locationAttributes().get( "topic" );
		bootstrapServers = locationAttributes().get( "bootstrap" );
		prop.put( "bootstrap.servers", kafkaTopicName );
		prop.put( "group.id", bootstrapServers );
		prop.put( "auto.commit.interval.ms", "1000" );
		prop.put( "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer" );
		prop.put( "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer" );
		final CommProtocol protocol = createProtocol();
		try( KafkaConsumer< String, String > consumer = new KafkaConsumer<>( prop ); ) {
			consumer.subscribe( Arrays.asList( kafkaTopicName ) );
			ConsumerRecords< String, String > records = consumer.poll( 1000L );
			KafkaCommChannel kafkaCommChannel = new KafkaCommChannel( inputPort.location(), protocol );
			kafkaCommChannel.setParentInputPort( inputPort );
			String message = "";
			for( ConsumerRecord< String, String > record : records ) {
				message = message + record.value();
			}
			byte[] byteToSend = message.getBytes( StandardCharsets.UTF_8 );
			KafkaMessage msg = new KafkaMessage( byteToSend );
			kafkaCommChannel.setData( msg );
			interpreter().commCore().scheduleReceive( kafkaCommChannel, inputPort() );
		}
	}

	@Override
	public void run() {
		while( true ) {
			try {
				synchronized( lock ) {
					while( true ) {
						lock.wait();
					}
				}
			} catch( InterruptedException ex ) {
				Logger.getLogger( KafkaCommListener.class.getName() ).log( Level.SEVERE, null, ex );
			}
		}
	}

	public Map< String, String > locationAttributes() throws IOException {
		return KafkaConnectionHandler.getConnection( inputPort().location() ).getLocationAttributes();
	}

	@Override
	public void shutdown() {
		try {
			// Close current connection.
			KafkaConnectionHandler.closeConnection( inputPort().location() );
		} catch( IOException ex ) {
			Logger.getLogger( KafkaConnectionHandler.class.getName() ).log( Level.WARNING, null, ex );
		}
	}
}
