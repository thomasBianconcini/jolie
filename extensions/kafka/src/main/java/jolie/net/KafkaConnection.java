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
		locationAttributes = new HashMap<>();
	}

	public void readAttributes() {

		locationAttributes = getAttributesMap( location.getQuery() );
	}

	public Map< String, String > getAttributesMap( String query ) {
		// kafka://localhost:8080?topic=nome_topic&.... valutare
		String splits[] = location.toString().split( "\\?" );
		Map< String, String > map = new HashMap();
		map.put( "bootstrap", splits[ 0 ] );
		String attributes[] = query.split( "&" );
		// ipotizzando solo topic da valutare altri parametri
		for( String attribute : attributes ) {
			String[] split = attribute.split( "=" );
			String name = split[ 0 ];
			String value = split.length >= 2 ? attribute.split( "=" )[ 1 ] : null;
			map.put( name, value );
		}
		return map;
	}

}
