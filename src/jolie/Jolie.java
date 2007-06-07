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

package jolie;

import java.io.FileNotFoundException;
import java.io.IOException;


/** Starter class of the Interpreter.
 * @author Fabrizio Montesi
 */
public class Jolie
{
	/** Entry point of program execution.
	 * 
	 * @param args The command line arguments.
	 * @todo Standardize the exit codes.
	 */
	public static void main( String[] args )
	{
		int exitCode = 0;
		try {
			(new Interpreter( args )).run();
		} catch( CommandLineException cle ) {
			System.out.println( cle.getMessage() );
		} catch( FileNotFoundException fe ) {
			System.out.println( fe.getMessage() );
			exitCode = 1;
		} catch( IOException ioe ) {
			ioe.printStackTrace();
			exitCode = 2;
		} catch( InterpreterException ie ) {
			ie.printStackTrace();
			exitCode = 3;
		} finally {
			System.exit( exitCode ); // This is also a workaround for InProcess java bug.
		}
	}
}
