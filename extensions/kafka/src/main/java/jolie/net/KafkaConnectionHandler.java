package jolie.net;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class KafkaConnectionHandler {
	private static final Map<URI, KafkaConnection > CONNECTIONS = new HashMap();

	public static KafkaConnection getConnection( URI location ) throws IOException {
		// If not already connected, do this.
		if( !CONNECTIONS.containsKey( location ) ) {
			setConnection( location );
		}

		return CONNECTIONS.get( location );
	}

	//Create and set a connection
	private static void setConnection( URI location ) throws IOException {
		CONNECTIONS.put( location, new KafkaConnection( location ) );
	}

	// Close a connection
	public static void closeConnection( URI location ) throws IOException {
		CONNECTIONS.get( location ).close();
		CONNECTIONS.remove( location );
	}
}
