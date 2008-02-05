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

package jolie.process;

import jolie.ExecutionThread;
import jolie.runtime.Expression;
import jolie.runtime.GlobalVariablePath;
import jolie.runtime.Value;

public class PostIncrementProcess implements Process, Expression
{
	private GlobalVariablePath varPath;

	public PostIncrementProcess( GlobalVariablePath varPath )
	{
		this.varPath = varPath;
	}
	
	public Process clone( TransformationReason reason )
	{
		return new PostIncrementProcess( varPath );
	}
	
	public void run()
	{
		if ( ExecutionThread.currentThread().isKilled() )
			return;
		Value val = varPath.getValue();
		val.setIntValue( val.intValue() + 1 );
	}
	
	public Value evaluate()
	{
		Value val = varPath.getValue();
		Value orig = Value.create( val.intValue() );
		val.setIntValue( val.intValue() + 1 );
		return orig;
	}
}
