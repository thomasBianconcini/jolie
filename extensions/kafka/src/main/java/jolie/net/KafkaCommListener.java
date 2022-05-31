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

public class KafkaCommListener extends CommListener {
	private final String kafkaTopicName;
	private final String bootstrapServers;
	private final String id;
	private final Properties prop = new Properties();
	CommProtocol protocol = null;
	private KafkaCommChannel kafkaCommChannel = null;
	KafkaConsumer< String, byte[] > consumerByte;
	KafkaConsumer< String, String > consumerString;
	private volatile boolean keepRun = true;

	private final String tipe;

	public KafkaCommListener( Interpreter interpreter, CommProtocolFactory protocolFactory, InputPort inputPort )
		throws IOException {
		super( interpreter, protocolFactory, inputPort );
		kafkaTopicName = locationAttributes().get( "topic" );
		bootstrapServers = locationAttributes().get( "bootstrap" );
		id = locationAttributes().get( "id" );
		tipe = locationAttributes().get( "type" );
		prop.put( "bootstrap.servers", bootstrapServers );
		prop.put( "group.id", id );
		prop.put( "auto.commit.interval.ms", "1000" );
		prop.put( "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer" );
		/*
		 * if( tipe.equals( "string" ) ) { prop.put( "value.deserializer",
		 * "org.apache.kafka.common.serialization.StringDeserializer" ); this.protocol = createProtocol();
		 * // Creation of Kafka Consumer this.consumerString = new KafkaConsumer<>( prop );
		 * consumerString.subscribe( Arrays.asList( kafkaTopicName ) ); kafkaCommChannel = new
		 * KafkaCommChannel( inputPort.location(), protocol ); kafkaCommChannel.setParentInputPort(
		 * inputPort ); } else {
		 */
		if( tipe.equals( "byte" ) ) {
			prop.put( "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer" );
			this.protocol = createProtocol();
			this.consumerByte = new KafkaConsumer<>( prop );
			// Creation of Kafka Consumer this.consumerByte = new KafkaConsumer<>( prop );
			consumerByte.subscribe( Arrays.asList( kafkaTopicName ) );
			kafkaCommChannel = new KafkaCommChannel( inputPort.location(), protocol );
			kafkaCommChannel.setParentInputPort( inputPort );
		} else if( tipe.equals( "string" ) ) {
			prop.put( "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer" );
			this.protocol = createProtocol();
			this.consumerString = new KafkaConsumer<>( prop );
			// Creation of Kafka Consumer this.consumerByte = new KafkaConsumer<>( prop );
			consumerString.subscribe( Arrays.asList( kafkaTopicName ) );
			kafkaCommChannel = new KafkaCommChannel( inputPort.location(), protocol );
			kafkaCommChannel.setParentInputPort( inputPort );
		}
		// }

	}

	@Override
	public void run() {
		while( keepRun ) {
			if( tipe.equals( "byte" ) ) {
				ConsumerRecords< String, byte[] > records = consumerByte.poll( Duration.ofSeconds( 1 ) );
				while( records.isEmpty() && keepRun ) {
					records = consumerByte.poll( Duration.ofSeconds( 0, 5 ) );
				}
				if( keepRun ) {
					for( ConsumerRecord< String, byte[] > record : records ) {
						// for each message in the topic
						byte[] byteToSend = record.value();
						KafkaMessage msg = new KafkaMessage( byteToSend );
						kafkaCommChannel.setData( msg );
						interpreter().commCore().scheduleReceive( kafkaCommChannel, inputPort() );
						// }
					}
				}
			} else if( tipe.equals( "string" ) ) {
				ConsumerRecords< String, String > records = consumerString.poll( Duration.ofSeconds( 1 ) );
				while( records.isEmpty() && keepRun ) {
					records = consumerString.poll( Duration.ofSeconds( 1 ) );
				}
				if( keepRun ) {
					for( ConsumerRecord< String, String > record : records ) {
						// for each message in the topic
						KafkaMessage msg = new KafkaMessage( record.value().getBytes() );
						kafkaCommChannel.setData( msg );
						interpreter().commCore().scheduleReceive( kafkaCommChannel, inputPort() );
						// }
					}
				}
			}
		}
		consumerByte.close();
		try {
			KafkaConnectionHandler.closeConnection( inputPort().location() );
		} catch( IOException e ) {
			throw new RuntimeException( e );
		}
	}

	// return the map of the attributes
	public Map< String, String > locationAttributes() throws IOException {
		return KafkaConnectionHandler.getConnection( inputPort().location() ).getLocationAttributes();
	}

	@Override
	public void shutdown() {
		keepRun = false;
	}
}
