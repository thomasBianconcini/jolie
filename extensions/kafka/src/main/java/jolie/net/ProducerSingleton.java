package jolie.net;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerSingleton {
	private static ProducerSingleton instance;
	private KafkaProducer< String, byte[] > byteProducer = null;
	private KafkaProducer< String, String > stringProducer = null;

	private ProducerSingleton( Properties prop, String tipe ) {
		if( tipe.equals( "byte" ) ) {
			this.byteProducer = new KafkaProducer<>( prop, new StringSerializer(), new ByteArraySerializer() );
		} else if( tipe.equals( "string" ) ) {
			this.stringProducer = new KafkaProducer<>( prop, new StringSerializer(), new StringSerializer() );
		}
	};

	public static synchronized ProducerSingleton getInstance( Properties prop, String tipe ) {
		if( instance == null ) {
			instance = new ProducerSingleton( prop, tipe );
		}
		return instance;
	}


	/*
	 * public static void close( String tipe ) { if( tipe.equals( "byte" ) ) { byteProducer.close(); }
	 * else if( tipe.equals( "string" ) ) { stringProducer.close(); } }
	 */


	public KafkaProducer< String, byte[] > getByteProducer() {
		return byteProducer;
	}

	public void setByteProducer( KafkaProducer< String, byte[] > byteProducer ) {
		this.byteProducer = byteProducer;
	}

	public KafkaProducer< String, String > getStringProducer() {
		return stringProducer;
	}

	public void setStringProducer(
		KafkaProducer< String, String > stringProducer ) {
		this.stringProducer = stringProducer;
	}
}
