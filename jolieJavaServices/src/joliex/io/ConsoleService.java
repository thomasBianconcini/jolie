/***************************************************************************
 *   Copyright (C) by Fabrizio Montesi                                     *
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

package joliex.io;


import java.io.BufferedReader;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ClosedByInterruptException;

import jolie.net.CommMessage;
import jolie.runtime.JavaService;
import jolie.runtime.Value;

public class ConsoleService extends JavaService
{
	private class ConsoleInputThread extends Thread
	{
		private boolean keepRun = true;
		
		public void kill()
		{
			keepRun = false;
			this.interrupt();
		}

		@Override
		public void run()
		{
			BufferedReader stdin =
				new BufferedReader(
						new InputStreamReader(
							Channels.newInputStream(
								(new FileInputStream( FileDescriptor.in )).getChannel() ) ) );
			try {
				String line;
				while( keepRun ) {
					line = stdin.readLine();
					_sendMessage( new CommMessage( "in", "/", Value.create( line ) ) );
				}
			} catch( ClosedByInterruptException ce ) {
			} catch( Exception e ) {
				e.printStackTrace();
			}
		}
	}
	
	private ConsoleInputThread consoleInputThread;
	
	public ConsoleService()
	{}

	public CommMessage registerForInput( CommMessage message )
	{
		consoleInputThread = new ConsoleInputThread();
		consoleInputThread.start();
		return CommMessage.createEmptyMessage();
	}
	
	@Override
	protected void finalize()
	{
		consoleInputThread.kill();
	}

	public CommMessage print( CommMessage message )
	{
		System.out.print( message.value().strValue() );
		return CommMessage.createEmptyMessage();
	}

	public CommMessage println( CommMessage message )
	{
		System.out.println( message.value().strValue() );
		return CommMessage.createEmptyMessage();
	}
}
