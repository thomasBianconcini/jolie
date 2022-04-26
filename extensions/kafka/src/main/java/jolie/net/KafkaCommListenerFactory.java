package jolie.net;

import jolie.Interpreter;
import jolie.net.ext.CommListenerFactory;
import jolie.net.ext.CommProtocolFactory;
import jolie.net.ports.InputPort;

import java.io.IOException;

public class KafkaCommListenerFactory extends CommListenerFactory {

	public KafkaCommListenerFactory( CommCore commCore ) {
		super( commCore );
	}

	@Override
	public CommListener createListener( Interpreter interpreter, CommProtocolFactory protocolFactory,
		InputPort inputPort ) throws IOException {
		return new KafkaCommListener( interpreter, protocolFactory, inputPort );
	}

}
