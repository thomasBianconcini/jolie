package jolie.net;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerSingleton {
	private KafkaProducer< String, byte[] > byteProducer = null;
	private static ProducerSingleton instance;

	private ProducerSingleton( Properties prop, String tipe ) {
		if( tipe.equals( "byte" ) ) {
			this.byteProducer = new KafkaProducer<>( prop, new StringSerializer(), new ByteArraySerializer() );
		}
	};

	public static synchronized ProducerSingleton getInstance( Properties prop, String tipe ) {
		if( instance == null ) {
			instance = new ProducerSingleton( prop, tipe );
		}
		return instance;
	}

	public KafkaProducer< String, byte[] > getByteProducer() {
		return byteProducer;
	}

}
