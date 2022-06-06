package jolie.net;

import jolie.net.ports.OutputPort;
import jolie.net.protocols.CommProtocol;
import jolie.runtime.Value;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
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
	final String tipe;

	BufferedWriter br = new BufferedWriter( new FileWriter( "/home/thomas/Desktop/temp.txt" ) );

	private final List< Long > responseWaiters = new ArrayList<>();

	public KafkaCommChannel( URI location, CommProtocol protocol ) throws IOException {
		super( location, protocol );
		this.location = location;
		this.message = null;
		kafkaTopicName = locationAttributes().get( "topic" );
		bootstrapServers = locationAttributes().get( "bootstrap" );
		tipe = locationAttributes().get( "type" );
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
			// boolean trovato = false;
			/*
			 * while( !trovato ) { for( Long l : responseWaiters ) { System.out.println( "long = " + l );
			 * System.out.println( "id = " + message.getId() ); System.out.println( "long = " + l ); if(
			 * l.doubleValue() == message.getId() ) { responseWaiters.remove( l ); System.out.println( "rec" +
			 * message.requestId() + "lunghezza =" + responseWaiters.size() ); br.append( (char)
			 * message.requestId() ); message = null; return CommMessage.createResponse( msg,
			 * Value.UNDEFINED_VALUE ); } } }
			 */
			System.out.println( "rec" + message.requestId() + "lunghezza =" + responseWaiters.size() );
			br.append( (char) message.requestId() );
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
		// creation of the Kafka Consumer
		if( parentPort() instanceof OutputPort ) {
			prop = new Properties();
			prop.put( "bootstrap.servers", bootstrapServers );
			prop.setProperty( "kafka.topic.name", kafkaTopicName );
			if( tipe.equals( "byte" ) ) {
				KafkaProducer< String, byte[] > producer =
					new KafkaProducer<>( this.prop, new StringSerializer(), new ByteArraySerializer() );
				ProducerRecord< String, byte[] > record =
					new ProducerRecord<>( prop.getProperty( "kafka.topic.name" ),
						ostream.toByteArray() );
				producer.send( (record) );
				producer.close();
			} else if( tipe.equals( "string" ) ) {
				KafkaProducer< String, String > producer =
					new KafkaProducer<>( this.prop, new StringSerializer(), new StringSerializer() );
				ProducerRecord< String, String > record =
					new ProducerRecord<>( prop.getProperty( "kafka.topic.name" ), ostream.toString() );
				producer.send( (record) );
				producer.close();
			}
			br.append( (char) message.requestId() );
		}

		responseWaiters.add( message.requestId() );
		ostream.flush();
	}

	public Map< String, String > locationAttributes() throws IOException {
		return KafkaConnectionHandler.getConnection( location ).getLocationAttributes();
	}

	public void setData( KafkaMessage data ) {
		this.data = data;
	}

}
