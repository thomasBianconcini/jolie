package jolie.net;

import jolie.net.ports.OutputPort;
import jolie.net.protocols.CommProtocol;
import jolie.runtime.Value;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.*;
import java.net.URI;
import java.util.*;

public class KafkaCommChannel extends StreamingCommChannel {
	// General
	private final URI location;
	private final List< Long > responseWaiters = new ArrayList<>();
	// Input
	private KafkaMessage data;
	// Output
	private CommMessage message;
	private Properties prop;
	final String kafkaTopicName;
	final String bootstrapServers;
	final String type;
	private static ProducerSingleton ps;
	private volatile boolean keepRun = false;

	KafkaProducer< String, byte[] > byteProducer;
	KafkaProducer< String, String > stringProducer;
	BufferedWriter br = new BufferedWriter( new FileWriter( "/home/thomas/Desktop/temp.txt" ) );

	public KafkaCommChannel( URI location, CommProtocol protocol ) throws IOException {
		super( location, protocol );
		this.location = location;
		this.message = null;
		kafkaTopicName = locationAttributes().get( "topic" );
		bootstrapServers = locationAttributes().get( "bootstrap" );
		type = locationAttributes().get( "type" );
		setToBeClosed( false );
	}

	@Override
	protected void closeImpl() throws IOException {
		if( type.equals( "byte" ) )
			byteProducer.close();
		else if( type.equals( "string" ) )
			stringProducer.close();
		KafkaConnectionHandler.closeConnection( location );
	}

	@Override
	protected CommMessage recvImpl() throws IOException {
		ByteArrayOutputStream ostream = new ByteArrayOutputStream();
		// if we are an Input Port
		if( data != null ) {
			ByteArrayInputStream istream = new ByteArrayInputStream( data.body );
			data = null;
			return protocol().recv( istream, ostream );
		}
		// if we are an Outputport
		if( message != null ) {
			CommMessage msg = message;
			while( !keepRun ) {
				for( long l : responseWaiters ) {
					if( l == message.requestId() )
						keepRun = true;
				}
			}
			keepRun = false;
			responseWaiters.remove( message.requestId() );
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
			if( type.equals( "byte" ) ) {
				ps = ProducerSingleton.getInstance( prop, type );
				byteProducer = ps.getByteProducer();
				ProducerRecord< String, byte[] > record =
					new ProducerRecord<>( prop.getProperty( "kafka.topic.name" ), ostream.toByteArray() );
				byteProducer.send( (record) );
			} else if( type.equals( "string" ) ) {
				ps = ProducerSingleton.getInstance( prop, type );
				stringProducer = ps.getStringProducer();
				ProducerRecord< String, String > record =
					new ProducerRecord<>( prop.getProperty( "kafka.topic.name" ), ostream.toString() );
				stringProducer.send( (record) );
			}
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

	public KafkaMessage getData() {
		return data;
	}
}
