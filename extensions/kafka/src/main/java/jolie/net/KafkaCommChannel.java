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
	// Output
	private CommMessage message;
	private Properties prop;

	final String kafkaTopicName;
	final String bootstrapServers;

	public KafkaCommChannel( URI location, CommProtocol protocol ) throws IOException {
		super( location, protocol );
		this.location = location;
		// add parametri
		this.message = null;
		kafkaTopicName = locationAttributes().get( "topic" );
		bootstrapServers = locationAttributes().get( "bootstrap" );
		setToBeClosed( false );
	}

	@Override
	protected void closeImpl() throws IOException {
		KafkaConnectionHandler.closeConnection( location );
	}

	@Override
	protected CommMessage recvImpl() throws IOException {
		ByteArrayOutputStream ostream = new ByteArrayOutputStream();
		// if we are an Input Port
		if( data != null ) {
			ByteArrayInputStream istream = new ByteArrayInputStream( data.body );
			return protocol().recv( istream, ostream );

		}
		// if we are an Outputport
		if( message != null ) {
			CommMessage msg = message;
			message = null;
			return CommMessage.createResponse( msg, Value.UNDEFINED_VALUE );

		}
		throw new IOException( "Wrong context for receive!" );
	}


	@Override
	protected void sendImpl( CommMessage message ) throws IOException {
		ByteArrayOutputStream ostream = new ByteArrayOutputStream();
		this.message = message;
		protocol().send( ostream, message, null );
		if( parentPort() instanceof OutputPort ) {
			prop = new Properties();
			prop.put( "bootstrap.servers", bootstrapServers );
			prop.setProperty( "kafka.topic.name", kafkaTopicName );
			KafkaProducer< String, byte[] > producer =
				new KafkaProducer<>( this.prop, new StringSerializer(), new ByteArraySerializer() );
			ProducerRecord< String, byte[] > record =
				new ProducerRecord<>( prop.getProperty( "kafka.topic.name" ),
					ostream.toByteArray() );
			producer.send( (record) );
			producer.close();

		}
		// insirisci condizione oneway o reqresp
		/*
		 * else if( parentPort() instanceof InputPort ) { prop = new Properties(); prop.put(
		 * "bootstrap.servers", bootstrapServers ); prop.setProperty( "kafka.topic.name", kafkaTopicName );
		 * KafkaProducer< String, byte[] > producer = new KafkaProducer<>( this.prop, new
		 * StringSerializer(), new ByteArraySerializer() ); ProducerRecord< String, byte[] > record = new
		 * ProducerRecord<>( prop.getProperty( "kafka.topic.name" ), this.data.toString().getBytes(
		 * StandardCharsets.UTF_8 ) ); producer.send( (record) ); producer.close();
		 * 
		 * }
		 */
		/*
		 * else { throw new IOException( "Port is of unexpected type!" ); }
		 */
		ostream.flush();
	}

	public Map< String, String > locationAttributes() throws IOException {
		return KafkaConnectionHandler.getConnection( location ).getLocationAttributes();
	}

	public void setData( KafkaMessage data ) {
		this.data = data;
	}

}
