/***************************************************************************
 *   Copyright (C) 2006-2016 by Fabrizio Montesi <famontesi@gmail.com>     *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU Library General Public License as       *
 *   published by the Free Software Foundation; either version 2 of the    *
 *   License, or (at your option) any later version.                       *
 *                                                                         *
 *   This program is distributed in the hope that it will be useful,       *
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of        *
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         *
 *   GNU General Public License for more details.                          *
 *                                                                         *
 *   You should have received a copy of the GNU Library General Public     *
 *   License along with this program; if not, write to the                 *
 *   Free Software Foundation, Inc.,                                       *
 *   59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.             *
 *                                                                         *
 *   For details about the authors of this software, see the AUTHORS file. *
 ***************************************************************************/

package jolie.net;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
//import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jolie.js.JsUtils;
import jolie.lang.NativeType;
import jolie.lang.parse.ast.types.BasicTypeDefinition;
import jolie.net.protocols.ConcurrentCommProtocol;
import jolie.runtime.ByteArray;
import jolie.runtime.FaultException;
import jolie.runtime.Value;
import jolie.runtime.ValueVector;
import jolie.runtime.VariablePath;
import jolie.runtime.typing.BasicType;
import jolie.runtime.typing.Type;
import jolie.util.Range;

public class SodepProtocol extends ConcurrentCommProtocol {
	private static class DataTypeHeaderId {
		private static final int NULL = 0;
		private static final int STRING = 1;
		private static final int INT = 2;
		private static final int DOUBLE = 3;
		private static final int BYTE_ARRAY = 4;
		private static final int BOOL = 5;
		private static final int LONG = 6;
	}

	public String name() {
		return "sodep";
	}

	private Charset stringCharset = StandardCharsets.UTF_8;

	private String readString( DataInput in )
		throws IOException {
		int len = in.readInt();
		if( len > 0 ) {
			byte[] bb = new byte[ len ];
			in.readFully( bb );
			return new String( bb, stringCharset );
		}
		return "";
	}

	private void writeString( DataOutput out, String str )
		throws IOException {
		if( str.isEmpty() ) {
			out.writeInt( 0 );
		} else {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			Writer writer = new OutputStreamWriter( bos, stringCharset );
			writer.write( str );
			writer.close();
			byte[] bb = bos.toByteArray();
			out.writeInt( bb.length );
			out.write( bb );
		}
	}

	private ByteArray readByteArray( DataInput in )
		throws IOException {
		int size = in.readInt();
		ByteArray ret;
		if( size > 0 ) {
			byte[] bytes = new byte[ size ];
			in.readFully( bytes, 0, size );
			ret = new ByteArray( bytes );
		} else {
			ret = new ByteArray( new byte[ 0 ] );
		}
		return ret;
	}

	private void writeByteArray( DataOutput out, ByteArray byteArray )
		throws IOException {
		int size = byteArray.size();
		out.writeInt( size );
		if( size > 0 ) {
			out.write( byteArray.getBytes() );
		}
	}

	private void writeFault( DataOutput out, FaultException fault )
		throws IOException {
		writeString( out, fault.faultName() );
		writeValue( out, fault.value() );
	}

	private void writeValue( DataOutput out, Value value )
		throws IOException {
		Object valueObject = value.valueObject();
		if( valueObject == null ) {
			out.writeByte( DataTypeHeaderId.NULL );
		} else if( valueObject instanceof String ) {
			out.writeByte( DataTypeHeaderId.STRING );
			writeString( out, (String) valueObject );
		} else if( valueObject instanceof Integer ) {
			out.writeByte( DataTypeHeaderId.INT );
			out.writeInt( (Integer) valueObject );
		} else if( valueObject instanceof Double ) {
			out.writeByte( DataTypeHeaderId.DOUBLE );
			out.writeDouble( (Double) valueObject );
		} else if( valueObject instanceof ByteArray ) {
			out.writeByte( DataTypeHeaderId.BYTE_ARRAY );
			writeByteArray( out, (ByteArray) valueObject );
		} else if( valueObject instanceof Boolean ) {
			out.writeByte( DataTypeHeaderId.BOOL );
			out.writeBoolean( (Boolean) valueObject );
		} else if( valueObject instanceof Long ) {
			out.writeByte( DataTypeHeaderId.LONG );
			out.writeLong( (Long) valueObject );
		} else {
			out.writeByte( DataTypeHeaderId.NULL );
		}

		Map< String, ValueVector > children = value.children();
		List< Entry< String, ValueVector > > entries =
			new LinkedList<>();
		// if ( !entry.getKey().startsWith( "@" ) ) {
		// }
		entries.addAll( children.entrySet() );

		out.writeInt( entries.size() );
		for( Entry< String, ValueVector > entry : entries ) {
			writeString( out, entry.getKey() );
			out.writeInt( entry.getValue().size() );
			for( Value v : entry.getValue() ) {
				writeValue( out, v );
			}
		}
	}

	private void writeMessage( DataOutput out, CommMessage message )
		throws IOException {
		out.writeLong( message.requestId() );
		writeString( out, message.resourcePath() );
		writeString( out, message.operationName() );
		FaultException fault = message.fault();
		if( fault == null ) {
			out.writeBoolean( false );
		} else {
			out.writeBoolean( true );
			writeFault( out, fault );
		}
		writeValue( out, message.value() );
	}

	private Value readValue( DataInput in )
		throws IOException {
		Value value = Value.create();
		Object valueObject = null;
		byte b = in.readByte();
		switch( b ) {
		case DataTypeHeaderId.STRING:
			valueObject = readString( in );
			break;
		case DataTypeHeaderId.INT:
			valueObject = in.readInt();
			break;
		case DataTypeHeaderId.LONG:
			valueObject = in.readLong();
			break;
		case DataTypeHeaderId.DOUBLE:
			valueObject = in.readDouble();
			break;
		case DataTypeHeaderId.BYTE_ARRAY:
			valueObject = readByteArray( in );
			break;
		case DataTypeHeaderId.BOOL:
			valueObject = in.readBoolean();
			break;
		case DataTypeHeaderId.NULL:
		default:
			break;
		}

		value.setValue( valueObject );

		Map< String, ValueVector > children = value.children();
		String s;
		int n, i, size, k;
		n = in.readInt(); // How many children?
		ValueVector vec;

		for( i = 0; i < n; i++ ) {
			s = readString( in );
			vec = ValueVector.create();
			size = in.readInt();
			for( k = 0; k < size; k++ ) {
				vec.add( readValue( in ) );
			}
			children.put( s, vec );
		}
		return value;
	}

	private FaultException readFault( DataInput in )
		throws IOException {
		String faultName = readString( in );
		Value value = readValue( in );
		return new FaultException( faultName, value );
	}

	private CommMessage readMessage( DataInput in )
		throws IOException {
		long id = in.readLong();
		String resourcePath = readString( in );
		String operationName = readString( in );
		FaultException fault = null;
		if( in.readBoolean() == true ) {
			fault = readFault( in );
		}
		Value value = readValue( in );
		return new CommMessage( id, operationName, resourcePath, value, fault );
	}

	public SodepProtocol( VariablePath configurationPath ) {
		super( configurationPath );
	}

	public void send( OutputStream ostream, CommMessage message, InputStream istream )
		throws IOException {
		channel().setToBeClosed( !checkBooleanParameter( "keepAlive", true ) );

		String charset = getStringParameter( "charset" );
		if( !charset.isEmpty() ) {
			stringCharset = Charset.forName( charset );
		}

		final DataOutputStream oos = new DataOutputStream( ostream );

		test( oos, message );
		if( 1 == 9 )
			writeMessage( oos, message );
	}

	public void test( DataOutputStream oos, CommMessage message ) throws IOException {
		StringBuilder json = new StringBuilder();
		Value value = Value.create();
		value.getChildren( "params" ).set( 0, message.value() );
		value.getFirstChild( "method" ).setValue( message.operationName() );
		value.getFirstChild( "sodepAsync" ).setValue( "1.0" );
		value.getFirstChild( "id" ).setValue( message.id() );
		value.getFirstChild( "resorucePath" ).setValue( message.resourcePath() );
		Type fullMessageType =
			Type.create( BasicType.fromBasicTypeDefinition( BasicTypeDefinition.of( NativeType.VOID ) ),
				new Range( 1, 1 ), false, null );
		jolie.js.JsUtils.valueToJsonString( value, true, fullMessageType, json );

		String jsonMessage = json.toString();
		oos.write( jsonMessage.getBytes() );
	}

	public CommMessage readJson( DataInput in ) throws IOException {
		Value value = Value.create();
		String json = in.readLine();
		JsUtils.parseJsonIntoValue( new InputStreamReader( new ByteArrayInputStream( json.getBytes() ) ), value,
			false );
		return new CommMessage( value.getFirstChild( "id" ).longValue(), value.getFirstChild( "method" ).strValue(),
			value.getFirstChild( "resourcePath" ).strValue(),
			value.getFirstChild( "params" ), null );
	}

	public CommMessage recv( InputStream istream, OutputStream ostream )
		throws IOException {
		channel().setToBeClosed( !checkBooleanParameter( "keepAlive", true ) );

		String charset = getStringParameter( "charset" );
		if( !charset.isEmpty() ) {
			stringCharset = Charset.forName( charset );
		}

		final DataInputStream ios = new DataInputStream( istream );
		if( 1 == 1 )
			return readJson( ios );
		else
			return readMessage( ios );
	}
}
