package jolie.net;

import jolie.net.ext.CommChannelFactory;
import jolie.net.ports.OutputPort;
import jolie.runtime.AndJarDeps;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@AndJarDeps( { "kafka-client.jar", "slf4j-api.jar" } )
public class KafkaCommChannelFactory extends CommChannelFactory {
	protected KafkaCommChannelFactory( CommCore commCore ) {
		super( commCore );
	}

	@Override
	public CommChannel createChannel( URI location, OutputPort port ) throws IOException {
		try {
			return new KafkaCommChannel( location, port.getProtocol() );
		} catch( URISyntaxException e ) {
			throw new IOException( e );
		}
	}
}
