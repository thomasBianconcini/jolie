package jolie.net;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class KafkaConnection {
	private final URI location;
	private Map< String, String > locationAttributes = null;

	public KafkaConnection( URI location ) throws IOException {
		this.location = location;
		readAttributes();
	}

	public Map< String, String > getLocationAttributes() {
		return locationAttributes;
	}

	public void close() {
		locationAttributes = null;
	}

	// set the Map locationAttributes from the string
	public void readAttributes() {
		locationAttributes = getAttributesMap( location.getQuery() );
	}

	public Map< String, String > getAttributesMap( String query ) {
		// location: "kafka://localhost:9092?topic=Test1&id=id"
		String splits[] = location.toString().split( "\\?" );
		Map< String, String > map = new HashMap();
		String boot = splits[ 0 ].split( "//" )[ 1 ];
		// boot= localhost:9092
		map.put( "bootstrap", boot );
		String attributes[] = query.split( "&" );
		for( String attribute : attributes ) {
			// topic=Test1&id=id
			String[] split = attribute.split( "=" );
			String name = split[ 0 ];
			String value = split.length >= 2 ? attribute.split( "=" )[ 1 ] : null;
			map.put( name, value );
		}
		return map;
	}

}
