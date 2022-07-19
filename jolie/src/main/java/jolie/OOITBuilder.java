/*
 * Copyright (C) 2006-2016 Fabrizio Montesi <famontesi@gmail.com>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */

package jolie;

import jolie.lang.Constants;
import jolie.lang.Keywords;
import jolie.lang.Constants.ExecutionMode;
import jolie.lang.Constants.OperandType;
import jolie.lang.parse.CorrelationFunctionInfo;
import jolie.lang.parse.CorrelationFunctionInfo.CorrelationPairInfo;
import jolie.lang.parse.OLParser;
import jolie.lang.parse.UnitOLVisitor;
import jolie.lang.parse.Scanner;
import jolie.lang.parse.ast.*;
import jolie.lang.parse.ast.CorrelationSetInfo.CorrelationVariableInfo;
import jolie.lang.parse.ast.ServiceNode.ParameterConfiguration;
import jolie.lang.parse.ast.courier.CourierChoiceStatement;
import jolie.lang.parse.ast.courier.CourierDefinitionNode;
import jolie.lang.parse.ast.courier.NotificationForwardStatement;
import jolie.lang.parse.ast.courier.SolicitResponseForwardStatement;
import jolie.lang.parse.ast.expression.*;
import jolie.lang.parse.ast.types.TypeChoiceDefinition;
import jolie.lang.parse.ast.types.TypeDefinition;
import jolie.lang.parse.ast.types.TypeDefinitionLink;
import jolie.lang.parse.ast.types.TypeInlineDefinition;
import jolie.lang.parse.context.ParsingContext;
import jolie.net.AggregatedOperation;
import jolie.net.ext.CommProtocolFactory;
import jolie.net.ports.InputPort;
import jolie.net.ports.Interface;
import jolie.net.ports.InterfaceExtender;
import jolie.net.ports.OutputPort;
import jolie.process.Process;
import jolie.process.*;
import jolie.process.courier.ForwardNotificationProcess;
import jolie.process.courier.ForwardSolicitResponseProcess;
import jolie.runtime.*;
import jolie.runtime.correlation.CorrelationSet;
import jolie.runtime.correlation.CorrelationSet.CorrelationPair;
import jolie.runtime.embedding.EmbeddedServiceLoader;
import jolie.runtime.embedding.EmbeddedServiceLoader.EmbeddedServiceConfiguration;
import jolie.runtime.embedding.EmbeddedServiceLoaderCreationException;
import jolie.runtime.expression.*;
import jolie.runtime.expression.Expression.Operand;
import jolie.runtime.typing.BasicType;
import jolie.runtime.typing.OneWayTypeDescription;
import jolie.runtime.typing.RequestResponseTypeDescription;
import jolie.runtime.typing.Type;
import jolie.runtime.typing.TypeCheckingException;
import jolie.util.ArrayListMultiMap;
import jolie.util.MultiMap;
import jolie.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.BiPredicate;

/**
 * Builds an interpretation tree by visiting a Jolie abstract syntax tree.
 * 
 * @author Fabrizio Montesi
 * @see UnitOLVisitor
 */
public class OOITBuilder implements UnitOLVisitor {
	private final Program program;
	private final Value initValue;
	private boolean valid = true;
	private final Interpreter interpreter;
	private String currentOutputPort = null;
	private Interface currentPortInterface = null;
	private final Map< String, Boolean > constantFlags;
	private final Map< String, InputPort > inputPorts = new HashMap<>();
	private final Map< String, ServiceNode > services = new HashMap<>();
	private final List< Pair< Type.TypeLink, TypeDefinition > > typeLinks = new ArrayList<>();
	private final CorrelationFunctionInfo correlationFunctionInfo;
	private ExecutionMode executionMode = Constants.ExecutionMode.SINGLE;
	private boolean registerSessionStarters = false;
	private InputPort currCourierInputPort = null;
	private String currCourierOperationName = null;
	private final Map< String, Map< String, AggregationConfiguration > > aggregationConfigurations =
		new HashMap<>(); // Input port name -> (operation name -> aggregation configuration)
	private final Map< String, InterfaceExtender > interfaceExtenders =
		new HashMap<>();
	private final Deque< OLSyntaxNode > lazyVisits = new LinkedList<>();
	private boolean firstPass = true;

	private static class AggregationConfiguration {
		private final OutputPort defaultOutputPort;
		private final Interface aggregatedInterface;
		private final InterfaceExtender interfaceExtender;

		public AggregationConfiguration(
			OutputPort defaultOutputPort,
			Interface aggregatedInterface,
			InterfaceExtender interfaceExtender ) {
			this.defaultOutputPort = defaultOutputPort;
			this.aggregatedInterface = aggregatedInterface;
			this.interfaceExtender = interfaceExtender;
		}
	}

	/**
	 * Constructor.
	 * 
	 * @param interpreter the Interpreter requesting the interpretation tree building
	 * @param program the Program to generate the interpretation tree from
	 * @param isConstantMap
	 * @param correlationFunctionInfo
	 * @see Program
	 */
	public OOITBuilder(
		Interpreter interpreter,
		Program program,
		Map< String, Boolean > isConstantMap,
		CorrelationFunctionInfo correlationFunctionInfo,
		Value initValue ) {
		this.interpreter = interpreter;
		this.constantFlags = isConstantMap;
		this.correlationFunctionInfo = correlationFunctionInfo;
		this.initValue = initValue;

		List< OLSyntaxNode > programChildren = new ArrayList<>();
		programChildren.addAll( OLParser.createTypeDeclarationMap( program.context() ).values() );
		programChildren.addAll( program.children() );
		this.program = new Program( program.context(), programChildren );
	}

	private static String buildErrorMessage( ParsingContext context, String message ) {
		return context.sourceName() + ":" + (context.startLine() + 1) + ": " + message;
	}

	private void error( ParsingContext context, String message ) {
		valid = false;
		interpreter.logSevere( buildErrorMessage( context, message ) );
	}

	private void error( ParsingContext context, Exception e ) {
		valid = false;
		interpreter.logSevere( new InterpreterException(
			buildErrorMessage( context, e.getMessage() ),
			e.getCause() ) );
	}

	private void error( ParsingContext context, String prefix, Exception e ) {
		valid = false;
		interpreter.logSevere( new InterpreterException(
			buildErrorMessage( context, prefix + " " + e.getMessage() ),
			e.getCause() ) );
	}

	/**
	 * Launches the build process.
	 * 
	 * The Program passed to the constructor gets visited and the Interpreter passed to the constructor
	 * is set with the necessary references to the interpretation tree.
	 * 
	 * @return true if the build process is successful, false otherwise
	 */
	public boolean build() {
		visit( program );
		if( services.values().isEmpty() ) {
			error( program.context(), "No services defined" );
			return false;
		}

		final ServiceNode mainService;
		final String executionTarget = interpreter.configuration().executionTarget();
		if( executionTarget != null ) {
			mainService = services.get( executionTarget );
			if( mainService == null ) {
				error( program.context(), "Service " + executionTarget + " is not defined" );
				return false;
			}
		} else if( services.values().size() > 1 ) {
			error( program.context(),
				"No service to be run has been specified, but multiple service definitions found. Run jolie --help to see how to use the -s parameter" );
			return false;
		} else {
			mainService = services.values().iterator().next();
		}

		if( mainService.parameterConfiguration().isPresent() ) {
			ParameterConfiguration config = mainService.parameterConfiguration().get();
			VariablePath paramPath = new VariablePathBuilder( false ).add( config.variablePath(), 0 ).toVariablePath();
			Type paramType = buildType( config.type() );
			resolveTypeLinks();
			try {
				paramType.check( interpreter.receivingEmbeddedValue() );
			} catch( TypeCheckingException e ) {
				error( mainService.context(), "The actual parameters passed to service " + mainService.name()
					+ " do not match the expected type.", e );
				return false;
			}
			new ClosedVariablePath( paramPath, initValue )
				.getValue().deepCopy( interpreter.receivingEmbeddedValue() );
		}

		visit( mainService.program() );

		resolveTypeLinks();
		checkForInit();
		lazyVisits();
		buildCorrelationSets();

		return valid;
	}

	private void lazyVisits() {
		firstPass = false;
		OLSyntaxNode node;
		while( (node = lazyVisits.poll()) != null ) {
			node.accept( this );
		}
	}

	private void visitLater( OLSyntaxNode n ) {
		lazyVisits.add( n );
	}

	private void checkForInit() {
		try {
			interpreter.getDefinition( "init" );
		} catch( InvalidIdException e ) {
			interpreter.register( "init",
				new InitDefinitionProcess(
					new ScopeProcess( "main", NullProcess.getInstance(), false ) ) );
			// new InstallProcess( SessionThread.createDefaultFaultHandlers( interpreter ) ), false ) ) );
		}
	}

	private void resolveTypeLinks() {
		Type linkedType;
		for( int i = 0; i < typeLinks.size(); i++ ) {
			Pair< Type.TypeLink, TypeDefinition > pair = typeLinks.get( i );

			linkedType = getOrBuildType( pair.value() );

			if( linkedType == null ) {
				error( pair.value().context(),
					"type link to " + pair.key().linkedTypeName() + " cannot be resolved" );
			} else {
				pair.key().setLinkedType( linkedType );
				typeLinks.remove( i );
				i--;
			}
		}
	}

	private void buildCorrelationSets() {
		Interpreter.SessionStarter starter;
		Collection< String > operations;
		VariablePath sessionVariablePath;
		VariablePath messageVariablePath;
		CorrelationSet currCorrelationSet;
		Set< Interpreter.SessionStarter > starters = new HashSet<>();
		List< VariablePath > correlationVariablePaths;
		MultiMap< String, CorrelationPair > correlationMap;

		for( CorrelationSetInfo csetInfo : correlationFunctionInfo.correlationSets() ) {
			correlationVariablePaths = new ArrayList<>();
			correlationMap = new ArrayListMultiMap<>();

			for( CorrelationVariableInfo csetVariableInfo : csetInfo.variables() ) {
				sessionVariablePath = buildCorrelationVariablePath( csetVariableInfo.correlationVariablePath() );
				correlationVariablePaths.add( sessionVariablePath );
			}
			operations = correlationFunctionInfo.correlationSetOperations().get( csetInfo );
			for( String operationName : operations ) {
				for( CorrelationPairInfo pairInfo : correlationFunctionInfo
					.getOperationCorrelationPairs( operationName ) ) {
					sessionVariablePath = buildCorrelationVariablePath( pairInfo.sessionPath() );
					messageVariablePath = buildVariablePath( pairInfo.messagePath() );
					correlationMap.put(
						operationName,
						new CorrelationPair( sessionVariablePath, messageVariablePath ) );
					starter = interpreter.getSessionStarter( operationName );
					if( starter != null && starter.correlationInitializer() == null ) {
						starters.add( starter );
					}
				}
			}

			currCorrelationSet = new CorrelationSet( correlationVariablePaths, correlationMap );
			interpreter.addCorrelationSet( currCorrelationSet );
			for( Interpreter.SessionStarter initStarter : starters ) {
				initStarter.setCorrelationInitializer( currCorrelationSet );
			}
			starters.clear();
		}
	}

	public void visit( ExecutionInfo n ) {
		executionMode = n.mode();
		interpreter.setExecutionMode( n.mode() );
	}

	public void visit( VariablePathNode n ) {}

	public void visit( CorrelationSetInfo n ) {
		// correlationSetInfoList.add( n );
	}

	public void visit( OutputPortInfo n ) {
		final boolean isConstant = constantFlags.computeIfAbsent( n.id(), k -> false );

		currentOutputPort = n.id();
		notificationTypes.put( currentOutputPort, new HashMap<>() );
		solicitResponseTypes.put( currentOutputPort, new HashMap<>() );
		for( OperationDeclaration decl : n.operations() ) {
			decl.accept( this );
		}
		currentOutputPort = null;

		Expression locationExpr = buildExpression( n.location() );

		if( locationExpr instanceof VariablePath ) {
			VariablePath path = new ClosedVariablePath( (VariablePath) locationExpr, initValue );
			locationExpr = path.getValue();
		}

		OLSyntaxNode protocolNode =
			ModuleSystemUtil.transformProtocolExpression( n.protocol(), interpreter.commCore() );
		Expression protocolExpr = buildExpression( protocolNode );

		if( protocolExpr instanceof VariablePath ) {
			VariablePath path = new ClosedVariablePath( (VariablePath) protocolExpr, initValue );
			protocolExpr = path.getValue();
		}

		interpreter.register( n.id(),
			new OutputPort(
				interpreter,
				n.id(),
				locationExpr,
				protocolExpr,
				getOutputPortInterface( n.id() ),
				isConstant ) );
	}

	private Interface getOutputPortInterface( String outputPortName ) {
		Map< String, OneWayTypeDescription > oneWayMap = notificationTypes.get( outputPortName );
		if( oneWayMap == null ) {
			oneWayMap = new HashMap<>();
		}
		Map< String, RequestResponseTypeDescription > rrMap = solicitResponseTypes.get( outputPortName );
		if( rrMap == null ) {
			rrMap = new HashMap<>();
		}
		return new Interface( oneWayMap, rrMap );
	}

	public void visit( EmbeddedServiceNode n ) {
		try {
			final VariablePath path =
				n.portId() == null ? null
					: interpreter.getOutputPort( n.portId() ).locationVariablePath();

			final EmbeddedServiceConfiguration embeddedServiceConfiguration =
				n.type().equals( Constants.EmbeddedServiceType.INTERNAL )
					? new EmbeddedServiceLoader.InternalEmbeddedServiceConfiguration( n.servicePath(),
						n.program() )
					: new EmbeddedServiceLoader.ExternalEmbeddedServiceConfiguration( n.type(), n.servicePath(),
						Optional.empty(), Optional.empty() );

			interpreter.addEmbeddedServiceLoader(
				EmbeddedServiceLoader.create(
					interpreter,
					embeddedServiceConfiguration,
					path ) );
		} catch( EmbeddedServiceLoaderCreationException e ) {
			error( n.context(), e );
		} catch( InvalidIdException e ) {
			error( n.context(), "could not find port " + n.portId() );
		}
	}

	private AggregationConfiguration getAggregationConfiguration( String inputPortName, String operationName ) {
		Map< String, AggregationConfiguration > map = aggregationConfigurations.get( inputPortName );
		if( map == null ) {
			return null;
		}
		return map.get( operationName );
	}

	private void putAggregationConfiguration( String inputPortName, String operationName,
		AggregationConfiguration configuration ) {
		Map< String, AggregationConfiguration > map =
			aggregationConfigurations.computeIfAbsent( inputPortName, k -> new HashMap<>() );
		map.put( operationName, configuration );
	}

	public void visit( InputPortInfo n ) {
		currentPortInterface = new Interface(
			new HashMap<>(),
			new HashMap<>() );
		for( OperationDeclaration op : n.operations() ) {
			op.accept( this );
		}

		Map< String, OutputPort > redirectionMap =
			new HashMap<>();
		OutputPort oPort = null;
		for( Entry< String, String > entry : n.redirectionMap().entrySet() ) {
			try {
				oPort = interpreter.getOutputPort( entry.getValue() );
			} catch( InvalidIdException e ) {
				error( n.context(),
					"Unknown output port (" + entry.getValue() + ") in redirection for input port " + n.id() );
			}
			redirectionMap.put( entry.getKey(), oPort );
		}

		OutputPort outputPort;
		Map< String, OneWayTypeDescription > outputPortNotificationTypes;
		Map< String, RequestResponseTypeDescription > outputPortSolicitResponseTypes;
		Map< String, AggregatedOperation > aggregationMap = new HashMap<>();
		InterfaceExtender extender;
		for( InputPortInfo.AggregationItemInfo item : n.aggregationList() ) {
			String outputPortName = item.outputPortList()[ 0 ];
			if( item.interfaceExtender() == null ) {
				extender = null;
			} else {
				extender = interfaceExtenders.get( item.interfaceExtender().name() );
			}
			try {
				outputPort = interpreter.getOutputPort( outputPortName );
				outputPortNotificationTypes = notificationTypes.get( outputPortName );
				outputPortSolicitResponseTypes = solicitResponseTypes.get( outputPortName );
				for( String operationName : outputPortNotificationTypes.keySet() ) {
					aggregationMap.put( operationName, AggregatedOperation.createDirect( operationName,
						Constants.OperationType.ONE_WAY, outputPort ) );
					putAggregationConfiguration( n.id(), operationName,
						new AggregationConfiguration( outputPort, outputPort.getInterface(), extender ) );
				}
				for( String operationName : outputPortSolicitResponseTypes.keySet() ) {
					aggregationMap.put( operationName, AggregatedOperation.createDirect( operationName,
						Constants.OperationType.REQUEST_RESPONSE, outputPort ) );
					putAggregationConfiguration( n.id(), operationName,
						new AggregationConfiguration( outputPort, outputPort.getInterface(), extender ) );
				}
			} catch( InvalidIdException e ) {
				error( n.context(), e );
			}
		}

		VariablePath locationPath =
			new VariablePathBuilder( true ).add( Constants.INPUT_PORTS_NODE_NAME, 0 )
				.add( n.id(), 0 ).add( Constants.LOCATION_NODE_NAME, 0 )
				.toClosedVariablePath( interpreter.globalValue() );

		VariablePath protocolPath =
			new VariablePathBuilder( true ).add( Constants.INPUT_PORTS_NODE_NAME, 0 )
				.add( n.id(), 0 ).add( Constants.PROTOCOL_NODE_NAME, 0 )
				.toClosedVariablePath( interpreter.globalValue() );

		// locationPath = new ClosedVariablePath( locationPath, interpreter.globalValue() );

		Expression locationExpr = buildExpression( n.location() );
		String locationStr = null;
		if( locationExpr instanceof Value ) {
			locationStr = ((Value) locationExpr).strValue();
		} else if( locationExpr instanceof VariablePath ) {
			VariablePath path = new ClosedVariablePath( (VariablePath) locationExpr, initValue );
			locationStr = path.getValue().strValue();
		}

		if( locationStr == null ) {
			error( n.context(), "location expression is not valid" );
			return;
		}
		locationPath.getValue().setValue( locationStr );

		String protocolStr = null;
		List< Process > protocolProcs = new ArrayList<>();
		if( n.protocol() != null ) {
			OLSyntaxNode protocolNode =
				ModuleSystemUtil.transformProtocolExpression( n.protocol(), interpreter.commCore() );
			Expression protocolExpr = buildExpression( protocolNode );
			protocolProcs.add( new DeepCopyProcess( protocolPath, protocolExpr, true, n.context() ) );
			if( protocolExpr == null ) {
				error( n.context(), "protocol expression is not valid" );
				return;
			}
			if( protocolExpr instanceof Value || protocolExpr instanceof InlineTreeExpression ) {
				protocolStr = protocolExpr.evaluate().strValue();

			} else if( protocolExpr instanceof VariablePath ) {
				VariablePath path = new ClosedVariablePath( (VariablePath) protocolExpr, initValue );
				protocolStr = path.getValue().strValue();
			}
		} else {
			protocolProcs.add( NullProcess.getInstance() );
		}
		SequentialProcess protocolConfigurationSequence =
			new SequentialProcess( protocolProcs.toArray( new Process[ 0 ] ) );

		CommProtocolFactory protocolFactory = null;

		try {
			protocolFactory = interpreter.commCore().getCommProtocolFactory( protocolStr );
		} catch( IOException e ) {
			error( n.context(), e );
		}

		InputPort inputPort = new InputPort(
			n.id(),
			locationPath,
			protocolPath,
			currentPortInterface,
			aggregationMap,
			redirectionMap );

		if( n.location().toString().equals( Constants.LOCAL_LOCATION_KEYWORD ) ) {
			try {
				interpreter.commCore().addLocalInputPort( inputPort );
				inputPorts.put( inputPort.name(), inputPort );
			} catch( IOException e ) {
				error( n.context(), e );
			}
		} else if( protocolFactory != null || locationStr.startsWith( Constants.LOCAL_LOCATION_KEYWORD ) ) {
			try {
				interpreter.commCore().addInputPort(
					inputPort,
					protocolFactory,
					protocolConfigurationSequence );
				inputPorts.put( inputPort.name(), inputPort );
			} catch( IOException ioe ) {
				error( n.context(), ioe );
			}
		} else {
			error( n.context(),
				"Communication protocol extension for protocol " + protocolStr + " of port " + n.id() + " not found." );
		}
		currentPortInterface = null;
	}

	private Process currProcess;
	private Expression currExpression;
	private Type currType;
	boolean insideType = false;

	private final Map< TypeDefinition, Type > typeMap = new HashMap<>();
	private final Map< String, Map< String, OneWayTypeDescription > > notificationTypes =
		new HashMap<>(); // Maps output ports to their OW operation types
	private final Map< String, Map< String, RequestResponseTypeDescription > > solicitResponseTypes =
		new HashMap<>(); // Maps output ports to their RR
							// operation types

	public void visit( TypeInlineDefinition n ) {
		boolean backupInsideType = insideType;
		insideType = true;

		if( n.untypedSubTypes() ) {
			currType = Type.create( BasicType.fromBasicTypeDefinition( n.basicType() ), n.cardinality(), true, null );
		} else {
			Map< String, Type > subTypes = new HashMap<>();
			if( n.subTypes() != null ) {
				for( Entry< String, TypeDefinition > entry : n.subTypes() ) {
					subTypes.put( entry.getKey(), buildType( entry.getValue() ) );
				}
			}
			currType = Type.create(
				BasicType.fromBasicTypeDefinition( n.basicType() ),
				n.cardinality(),
				false,
				subTypes );
		}

		insideType = backupInsideType;

		if( insideType == false && insideOperationDeclarationOrInstanceOf == false ) {
			typeMap.put( n, currType );
		}
	}

	public void visit( TypeDefinitionLink n ) {
		Type.TypeLink link = Type.createLink( n.linkedTypeName(), n.cardinality() );
		currType = link;
		typeLinks.add( new Pair<>( link, n.linkedType() ) );

		if( insideType == false && insideOperationDeclarationOrInstanceOf == false ) {
			typeMap.put( n, currType );
		}
	}

	public void visit( Program p ) {
		for( OLSyntaxNode node : p.children() )
			node.accept( this );
	}

	private boolean insideOperationDeclarationOrInstanceOf = false;

	private OneWayTypeDescription buildOneWayTypeDescription( OneWayOperationDeclaration decl ) {
		if( decl == null ) {
			return null;
		}

		return new OneWayTypeDescription( getOrBuildType( decl.requestType() ) );
	}

	private RequestResponseTypeDescription buildRequestResponseTypeDescription(
		RequestResponseOperationDeclaration decl ) {
		if( decl == null ) {
			return null;
		}

		RequestResponseTypeDescription typeDescription;
		Map< String, Type > faults = new HashMap<>();
		for( Entry< String, TypeDefinition > entry : decl.faults().entrySet() ) {
			faults.put( entry.getKey(), getOrBuildType( entry.getValue() ) );
		}
		typeDescription = new RequestResponseTypeDescription(
			getOrBuildType( decl.requestType() ),
			getOrBuildType( decl.responseType() ),
			faults );
		return typeDescription;
	}

	public void visit( OneWayOperationDeclaration decl ) {
		boolean backup = insideOperationDeclarationOrInstanceOf;
		insideOperationDeclarationOrInstanceOf = true;
		OneWayTypeDescription typeDescription;
		if( currentOutputPort == null ) { // We are in an input port
			// Register if not already present
			typeDescription = buildOneWayTypeDescription( decl );
			try {
				interpreter.getOneWayOperation( decl.id() );
			} catch( InvalidIdException e ) {
				interpreter.register( decl.id(),
					new OneWayOperation( decl.id(),
						getOrBuildType( decl.requestType() ) ) );
			}
		} else {
			typeDescription = buildOneWayTypeDescription( decl );
			notificationTypes.get( currentOutputPort ).put( decl.id(), typeDescription );
		}

		if( currentPortInterface != null ) {
			currentPortInterface.oneWayOperations().put( decl.id(), typeDescription );
		}

		insideOperationDeclarationOrInstanceOf = backup;
	}

	public void visit( RequestResponseOperationDeclaration decl ) {
		boolean backup = insideOperationDeclarationOrInstanceOf;
		insideOperationDeclarationOrInstanceOf = true;
		RequestResponseTypeDescription typeDescription;
		if( currentOutputPort == null ) {
			// Register if not already present
			try {
				final RequestResponseOperation op = interpreter.getRequestResponseOperation( decl.id() );
				typeDescription = op.typeDescription();
			} catch( InvalidIdException e ) {
				typeDescription = buildRequestResponseTypeDescription( decl );
				interpreter.register(
					decl.id(),
					new RequestResponseOperation( decl.id(), typeDescription ) );
			}
		} else {
			typeDescription = buildRequestResponseTypeDescription( decl );
			solicitResponseTypes.get( currentOutputPort ).put( decl.id(), typeDescription );
		}

		if( currentPortInterface != null ) {
			currentPortInterface.requestResponseOperations().put( decl.id(), typeDescription );
		}
		insideOperationDeclarationOrInstanceOf = backup;

	}

	public void visit( DefinitionNode n ) {
		final DefinitionProcess def;

		switch( n.id() ) {
		case "main":
			switch( executionMode ) {
			case SINGLE:
				// We are a single-session service, so we will not spawn sessions
				registerSessionStarters = false;
				def = new DefinitionProcess( buildProcess( n.body() ) );
				break;
			default:
				registerSessionStarters = true;
				def = new DefinitionProcess( buildProcess( n.body() ) );
				registerSessionStarters = false;
				break;
			}
			break;
		case "init":
			final Process[] initChildren = {
				// new InstallProcess( SessionThread.createDefaultFaultHandlers( interpreter ) ),
				buildProcess( n.body() )
			};
			def = new InitDefinitionProcess( new ScopeProcess( "main", new SequentialProcess( initChildren ), false ) );
			break;

		default:
			def = new DefinitionProcess( buildProcess( n.body() ) );
			break;
		}

		interpreter.register( n.id(), def );
	}

	public void visit( ParallelStatement n ) {
		Process[] children = new Process[ n.children().size() ];
		int i = 0;
		for( OLSyntaxNode node : n.children() ) {
			node.accept( this );
			children[ i++ ] = currProcess;
		}
		currProcess = new ParallelProcess( children );
	}

	public void visit( SynchronizedStatement n ) {
		n.body().accept( this );
		currProcess = new SynchronizedProcess( n.id(), currProcess );
	}

	@Override
	public void visit( SequenceStatement n ) {
		final boolean origRegisterSessionStarters = registerSessionStarters;
		registerSessionStarters = false;

		final List< Process > children = new ArrayList<>( n.children().size() );
		n.children().forEach( ( child ) -> children.add( buildProcess( child ) ) );
		currProcess = new SequentialProcess( children.toArray( new Process[ 0 ] ) );

		if( origRegisterSessionStarters && children.get( 0 ) instanceof InputOperationProcess ) {
			// We must register this sequence as a starter guarded by the first input process
			InputOperationProcess first = (InputOperationProcess) children.remove( 0 );
			registerSessionStarter(
				first,
				new SequentialProcess( children.toArray( new Process[ 0 ] ) ) );
		}

		registerSessionStarters = origRegisterSessionStarters;
	}

	private void registerSessionStarter( InputOperationProcess guard, Process body ) {
		guard.setSessionStarter( true );
		interpreter.registerSessionStarter( guard, body );
	}

	@SuppressWarnings( "unchecked" )
	public void visit( NDChoiceStatement n ) {
		boolean origRegisterSessionStarters = registerSessionStarters;
		registerSessionStarters = false;

		List< Pair< InputOperationProcess, Process > > branches =
			new ArrayList<>( n.children().size() );
		InputOperationProcess guard;
		for( Pair< OLSyntaxNode, OLSyntaxNode > pair : n.children() ) {
			pair.key().accept( this );
			try {
				guard = (InputOperationProcess) currProcess;
				pair.value().accept( this );
				branches.add(
					new Pair<>( guard, currProcess ) );
				if( origRegisterSessionStarters ) {
					registerSessionStarter( guard, currProcess );
				}
			} catch( Exception e ) {
				e.printStackTrace();
			}
		}

		currProcess = new NDChoiceProcess( branches.toArray( new Pair[ 0 ] ) );

		registerSessionStarters = origRegisterSessionStarters;
	}

	public void visit( OneWayOperationStatement n ) {
		boolean origRegisterSessionStarters = registerSessionStarters;
		registerSessionStarters = false;

		try {
			InputOperationProcess inputProcess;
			currProcess = inputProcess =
				new OneWayProcess(
					interpreter.getOneWayOperation( n.id() ),
					buildVariablePath( n.inputVarPath() ),
					n.context() );
			if( origRegisterSessionStarters ) {
				registerSessionStarter( inputProcess, NullProcess.getInstance() );
			}
		} catch( InvalidIdException e ) {
			error( n.context(), e );
		}

		registerSessionStarters = origRegisterSessionStarters;
	}

	public void visit( RequestResponseOperationStatement n ) {
		boolean origRegisterSessionStarters = registerSessionStarters;
		registerSessionStarters = false;

		Expression outputExpression = null;
		if( n.outputExpression() != null ) {
			n.outputExpression().accept( this );
			outputExpression = currExpression;
		}

		try {
			InputOperationProcess inputProcess;
			currProcess = inputProcess =
				new RequestResponseProcess(
					interpreter.getRequestResponseOperation( n.id() ),
					buildVariablePath( n.inputVarPath() ),
					outputExpression,
					buildProcess( n.process() ),
					n.context() );
			if( origRegisterSessionStarters ) {
				registerSessionStarter( inputProcess, NullProcess.getInstance() );
			}
		} catch( InvalidIdException e ) {
			error( n.context(), e );
		}

		registerSessionStarters = origRegisterSessionStarters;
	}

	public void visit( NotificationOperationStatement n ) {
		Expression outputExpression = null;
		if( n.outputExpression() != null ) {
			n.outputExpression().accept( this );
			outputExpression = currExpression;
		}
		try {
			currProcess =
				new NotificationProcess(
					n.id(),
					interpreter.getOutputPort( n.outputPortId() ),
					outputExpression,
					notificationTypes.get( n.outputPortId() ).get( n.id() ),
					n.context() );
		} catch( InvalidIdException e ) {
			error( n.context(), e );
		}
	}

	public void visit( SolicitResponseOperationStatement n ) {
		try {
			Process installProcess = NullProcess.getInstance();
			if( n.handlersFunction() != null )
				installProcess = new InstallProcess( getHandlersFunction( n.handlersFunction() ) );
			currProcess =
				new SolicitResponseProcess(
					n.id(),
					interpreter.getOutputPort( n.outputPortId() ),
					buildExpression( n.outputExpression() ),
					buildVariablePath( n.inputVarPath() ),
					installProcess,
					solicitResponseTypes.get( n.outputPortId() ).get( n.id() ),
					n.context() );
		} catch( InvalidIdException e ) {
			error( n.context(), e );
		}
	}

	public void visit( LinkInStatement n ) {
		currProcess = new LinkInProcess( n.id() );
	}

	public void visit( LinkOutStatement n ) {
		currProcess = new LinkOutProcess( n.id() );
	}

	public void visit( ThrowStatement n ) {
		Expression expression = null;
		if( n.expression() != null ) {
			n.expression().accept( this );
			expression = currExpression;
		}
		currProcess = new ThrowProcess( n.id(), expression );
	}

	public void visit( CompensateStatement n ) {
		currProcess = new CompensateProcess( n.id() );
	}

	public void visit( Scope n ) {
		n.body().accept( this );
		currProcess = new ScopeProcess( n.id(), currProcess );
	}

	public void visit( InstallStatement n ) {
		currProcess = new InstallProcess( getHandlersFunction( n.handlersFunction() ) );
	}

	private List< Pair< String, Process > > getHandlersFunction( InstallFunctionNode n ) {
		List< Pair< String, Process > > pairs = new ArrayList<>( n.pairs().length );
		for( Pair< String, OLSyntaxNode > pair : n.pairs() ) {
			pair.value().accept( this );
			pairs.add( new Pair<>( pair.key(), currProcess ) );
		}
		return pairs;
	}

	public void visit( AssignStatement n ) {
		n.expression().accept( this );

		AssignmentProcess p =
			new AssignmentProcess(
				buildVariablePath( n.variablePath() ),
				currExpression, n.context() );
		currProcess = p;
		currExpression = p;
	}

	public void visit( AddAssignStatement n ) {
		n.expression().accept( this );

		AddAssignmentProcess p =
			new AddAssignmentProcess(
				buildVariablePath( n.variablePath() ),
				currExpression );
		currProcess = p;
		currExpression = p;
	}

	public void visit( SubtractAssignStatement n ) {
		n.expression().accept( this );

		SubtractAssignmentProcess p =
			new SubtractAssignmentProcess(
				buildVariablePath( n.variablePath() ),
				currExpression, n.context() );
		currProcess = p;
		currExpression = p;
	}

	public void visit( MultiplyAssignStatement n ) {
		n.expression().accept( this );

		MultiplyAssignmentProcess p =
			new MultiplyAssignmentProcess(
				buildVariablePath( n.variablePath() ),
				currExpression );
		currProcess = p;
		currExpression = p;
	}

	public void visit( DivideAssignStatement n ) {
		n.expression().accept( this );

		DivideAssignmentProcess p =
			new DivideAssignmentProcess(
				buildVariablePath( n.variablePath() ),
				currExpression );
		currProcess = p;
		currExpression = p;
	}

	private VariablePath buildCorrelationVariablePath( VariablePathNode path ) {
		VariablePathNode csetVarPathNode = new VariablePathNode( path.context(), VariablePathNode.Type.CSET );
		csetVarPathNode.append( new Pair<>(
			new ConstantStringExpression( path.context(), Keywords.CSETS ),
			new ConstantIntegerExpression( path.context(), 0 ) ) );
		csetVarPathNode.path().addAll( path.path() );
		return buildVariablePath( csetVarPathNode );
	}

	private VariablePath buildVariablePath( VariablePathNode path ) {
		if( path == null )
			return null;

		final Expression backupExpr = currExpression;

		@SuppressWarnings( "unchecked" )
		Pair< Expression, Expression >[] internalPath = new Pair[ path.path().size() ];
		int i = 0;
		for( Pair< OLSyntaxNode, OLSyntaxNode > pair : path.path() ) {
			pair.key().accept( this );
			Expression keyExpr = currExpression;
			if( pair.value() != null ) {
				pair.value().accept( this );
			} else {
				currExpression = null;
			}
			internalPath[ i++ ] = new Pair<>( keyExpr, currExpression );
		}

		currExpression = backupExpr;

		return path.isGlobal() ? new GlobalVariablePath( internalPath )
			: new VariablePath( internalPath );
	}

	public void visit( PointerStatement n ) {
		currProcess =
			new MakePointerProcess(
				buildVariablePath( n.leftPath() ),
				buildVariablePath( n.rightPath() ), n.context() );
	}

	public void visit( DeepCopyStatement n ) {
		currProcess =
			new DeepCopyProcess(
				buildVariablePath( n.leftPath() ),
				buildExpression( n.rightExpression() ),
				n.copyLinks(), n.context() );
	}

	public void visit( IfStatement n ) {
		IfProcess.CPPair[] pairs = new IfProcess.CPPair[ n.children().size() ];
		Process elseProcess = null;

		Expression condition;
		int i = 0;
		for( Pair< OLSyntaxNode, OLSyntaxNode > pair : n.children() ) {
			pair.key().accept( this );
			condition = currExpression;
			pair.value().accept( this );
			pairs[ i++ ] = new IfProcess.CPPair( condition, currProcess );
		}

		if( n.elseProcess() != null ) {
			n.elseProcess().accept( this );
			elseProcess = currProcess;
		}

		currProcess = new IfProcess( pairs, elseProcess );
	}

	public void visit( CurrentHandlerStatement n ) {
		currProcess = CurrentHandlerProcess.getInstance();
	}

	public void visit( DefinitionCallStatement n ) {
		currProcess = new CallProcess( n.id() );
	}

	public void visit( RunStatement n ) {
		n.expression().accept( this );
		currProcess = new RunProcess( currExpression );
	}

	public void visit( WhileStatement n ) {
		n.condition().accept( this );
		Expression condition = currExpression;
		n.body().accept( this );
		currProcess = new WhileProcess( condition, currProcess );
	}

	public void visit( OrConditionNode n ) {
		Expression[] children = new Expression[ n.children().size() ];
		int i = 0;
		for( OLSyntaxNode node : n.children() ) {
			node.accept( this );
			children[ i++ ] = currExpression;
		}
		currExpression = new OrCondition( children );
	}

	public void visit( AndConditionNode n ) {
		Expression[] children = new Expression[ n.children().size() ];
		int i = 0;
		for( OLSyntaxNode node : n.children() ) {
			node.accept( this );
			children[ i++ ] = currExpression;
		}
		currExpression = new AndCondition( children );
	}

	public void visit( IfExpressionNode n ) {
		currExpression = new IfExpression( buildExpression( n.guard() ), buildExpression( n.thenExpression() ),
			buildExpression( n.elseExpression() ) );
	}

	public void visit( NotExpressionNode n ) {
		n.expression().accept( this );
		currExpression = new NotExpression( currExpression );
	}

	public void visit( CompareConditionNode n ) {
		n.leftExpression().accept( this );
		Expression left = currExpression;
		n.rightExpression().accept( this );
		Scanner.TokenType opType = n.opType();

		final BiPredicate< Value, Value > operator =
			opType == Scanner.TokenType.EQUAL ? CompareOperators.EQUAL
				: opType == Scanner.TokenType.NOT_EQUAL ? CompareOperators.NOT_EQUAL
					: opType == Scanner.TokenType.LANGLE ? CompareOperators.MINOR
						: opType == Scanner.TokenType.RANGLE ? CompareOperators.MAJOR
							: opType == Scanner.TokenType.MINOR_OR_EQUAL ? CompareOperators.MINOR_OR_EQUAL
								: opType == Scanner.TokenType.MAJOR_OR_EQUAL ? CompareOperators.MAJOR_OR_EQUAL
									: null;
		Objects.requireNonNull( operator );
		currExpression = new CompareCondition( left, currExpression, operator );
	}

	public void visit( FreshValueExpressionNode n ) {
		currExpression = FreshValueExpression.getInstance();
	}

	public void visit( ConstantIntegerExpression n ) {
		currExpression = Value.create( n.value() );
	}

	public void visit( ConstantLongExpression n ) {
		currExpression = Value.create( n.value() );
	}

	public void visit( ConstantBoolExpression n ) {
		currExpression = Value.create( n.value() );
	}

	public void visit( ConstantDoubleExpression n ) {
		currExpression = Value.create( n.value() );
	}

	public void visit( ConstantStringExpression n ) {
		currExpression = Value.create( n.value() );
	}

	public void visit( ProductExpressionNode n ) {
		Operand[] operands = new Operand[ n.operands().size() ];
		int i = 0;
		for( Pair< OperandType, OLSyntaxNode > pair : n.operands() ) {
			pair.value().accept( this );
			operands[ i++ ] = new Operand( pair.key(), currExpression );
		}

		currExpression = new ProductExpression( operands );
	}

	public void visit( SumExpressionNode n ) {
		Operand[] operands = new Operand[ n.operands().size() ];
		int i = 0;
		for( Pair< OperandType, OLSyntaxNode > pair : n.operands() ) {
			pair.value().accept( this );
			operands[ i++ ] = new Operand( pair.key(), currExpression );
		}

		currExpression = new SumExpression( operands );
	}

	public void visit( VariableExpressionNode n ) {
		currExpression = buildVariablePath( n.variablePath() );
	}

	public void visit( InstallFixedVariableExpressionNode n ) {
		currExpression =
			new InstallFixedVariablePath(
				buildVariablePath( n.variablePath() ) );
	}

	public void visit( NullProcessStatement n ) {
		currProcess = NullProcess.getInstance();
	}

	public void visit( ExitStatement n ) {
		currProcess = ExitProcess.getInstance();
	}

	public void visit( VoidExpressionNode n ) {
		currExpression = new VoidExpression();
	}

	public void visit( ValueVectorSizeExpressionNode n ) {
		currExpression = new ValueVectorSizeExpression( buildVariablePath( n.variablePath() ) );
	}

	public void visit( PreDecrementStatement n ) {
		PreDecrementProcess p =
			new PreDecrementProcess( buildVariablePath( n.variablePath() ) );
		currProcess = p;
		currExpression = p;
	}

	public void visit( PostDecrementStatement n ) {
		PostDecrementProcess p =
			new PostDecrementProcess( buildVariablePath( n.variablePath() ) );
		currProcess = p;
		currExpression = p;
	}

	public void visit( IsTypeExpressionNode n ) {
		IsTypeExpressionNode.CheckType type = n.type();
		if( type == IsTypeExpressionNode.CheckType.DEFINED ) {
			currExpression =
				new IsDefinedExpression( buildVariablePath( n.variablePath() ) );
		} else if( type == IsTypeExpressionNode.CheckType.INT ) {
			currExpression =
				new IsIntExpression( buildVariablePath( n.variablePath() ) );
		} else if( type == IsTypeExpressionNode.CheckType.DOUBLE ) {
			currExpression =
				new IsDoubleExpression( buildVariablePath( n.variablePath() ) );
		} else if( type == IsTypeExpressionNode.CheckType.BOOL ) {
			currExpression =
				new IsBoolExpression( buildVariablePath( n.variablePath() ) );
		} else if( type == IsTypeExpressionNode.CheckType.LONG ) {
			currExpression =
				new IsLongExpression( buildVariablePath( n.variablePath() ) );
		} else if( type == IsTypeExpressionNode.CheckType.STRING ) {
			currExpression =
				new IsStringExpression( buildVariablePath( n.variablePath() ) );
		}
	}

	private InlineTreeExpression.Operation buildOperation( ParsingContext context,
		InlineTreeExpressionNode.Operation operation ) {
		if( operation instanceof InlineTreeExpressionNode.AssignmentOperation ) {
			InlineTreeExpressionNode.AssignmentOperation op = (InlineTreeExpressionNode.AssignmentOperation) operation;
			return new InlineTreeExpression.AssignmentOperation(
				buildVariablePath( op.path() ),
				buildExpression( op.expression() ) );
		} else if( operation instanceof InlineTreeExpressionNode.DeepCopyOperation ) {
			InlineTreeExpressionNode.DeepCopyOperation op = (InlineTreeExpressionNode.DeepCopyOperation) operation;
			return new InlineTreeExpression.DeepCopyOperation(
				buildVariablePath( op.path() ),
				buildExpression( op.expression() ) );
		} else if( operation instanceof InlineTreeExpressionNode.PointsToOperation ) {
			InlineTreeExpressionNode.PointsToOperation op = (InlineTreeExpressionNode.PointsToOperation) operation;
			return new InlineTreeExpression.PointsToOperation(
				buildVariablePath( op.path() ),
				buildVariablePath( op.target() ) );
		}

		error( context, "incomplete case analysis for inline tree operations" );
		throw new IllegalStateException( "incomplete case analysis for inline tree operations" );
	}

	public void visit( InlineTreeExpressionNode n ) {
		Expression rootExpression = buildExpression( n.rootExpression() );

		InlineTreeExpression.Operation[] operations = new InlineTreeExpression.Operation[ n.operations().length ];

		int i = 0;
		for( InlineTreeExpressionNode.Operation operation : n.operations() ) {
			operations[ i++ ] = buildOperation( n.context(), operation );
		}

		currExpression = new InlineTreeExpression( rootExpression, operations );
	}

	public void visit( InstanceOfExpressionNode n ) {
		insideOperationDeclarationOrInstanceOf = true;
		currExpression = new InstanceOfExpression( buildExpression( n.expression() ),
			getOrBuildType( n.type() ) );
		insideOperationDeclarationOrInstanceOf = false;
	}

	public void visit( TypeCastExpressionNode n ) {
		n.expression().accept( this );
		switch( n.type() ) {
		case INT:
			currExpression = new CastIntExpression( currExpression );
			break;
		case DOUBLE:
			currExpression = new CastDoubleExpression( currExpression );
			break;
		case STRING:
			currExpression = new CastStringExpression( currExpression );
			break;
		case BOOL:
			currExpression = new CastBoolExpression( currExpression );
			break;
		case LONG:
			currExpression = new CastLongExpression( currExpression );
			break;
		default:
			error( n.context(), "unsupported type for cast: " + n.type().toString() );
		}
	}

	public void visit( PreIncrementStatement n ) {
		PreIncrementProcess p =
			new PreIncrementProcess( buildVariablePath( n.variablePath() ) );
		currProcess = p;
		currExpression = p;
	}

	public void visit( PostIncrementStatement n ) {
		PostIncrementProcess p =
			new PostIncrementProcess( buildVariablePath( n.variablePath() ) );
		currProcess = p;
		currExpression = p;
	}

	public void visit( ForStatement n ) {
		n.init().accept( this );
		Process init = currProcess;
		n.post().accept( this );
		Process post = currProcess;
		n.condition().accept( this );
		Expression condition = currExpression;
		n.body().accept( this );
		currProcess = new ForProcess( init, condition, post, currProcess );
	}

	public void visit( ForEachArrayItemStatement n ) {
		n.body().accept( this );
		currProcess =
			new ForEachArrayItemProcess(
				buildVariablePath( n.keyPath() ),
				buildVariablePath( n.targetPath() ),
				currProcess );
	}

	public void visit( ForEachSubNodeStatement n ) {
		n.body().accept( this );
		currProcess =
			new ForEachSubNodeProcess(
				buildVariablePath( n.keyPath() ),
				buildVariablePath( n.targetPath() ),
				currProcess );
	}

	private Expression buildExpression( OLSyntaxNode n ) {
		if( n == null ) {
			return null;
		}
		n.accept( this );
		return currExpression;
	}

	private Process buildProcess( OLSyntaxNode n ) {
		if( n == null ) {
			return null;
		}
		n.accept( this );
		return currProcess;
	}

	private Type buildType( OLSyntaxNode n ) {
		if( n == null ) {
			return null;
		}
		n.accept( this );
		return currType;
	}

	private Type getOrBuildType( TypeDefinition typeDefinition ) {
		if( typeMap.containsKey( typeDefinition ) ) {
			return typeMap.get( typeDefinition );
		}
		return buildType( typeDefinition );
	}

	public void visit( SpawnStatement n ) {
		Process[] children = new Process[ 2 ];
		children[ 0 ] = new InstallProcess( SessionThread.createDefaultFaultHandlers( interpreter ) );
		children[ 1 ] = buildProcess( n.body() );
		currProcess = new SpawnProcess(
			buildVariablePath( n.indexVariablePath() ),
			buildExpression( n.upperBoundExpression() ),
			buildVariablePath( n.inVariablePath() ),
			new SequentialProcess( children ) );
	}

	public void visit( UndefStatement n ) {
		currProcess = new UndefProcess( buildVariablePath( n.variablePath() ) );
	}

	public void visit( InterfaceDefinition n ) {}

	public void visit( DocumentationComment n ) {}

	public void visit( InterfaceExtenderDefinition n ) {
		Map< String, OneWayTypeDescription > oneWayDescs = new HashMap<>();
		Map< String, RequestResponseTypeDescription > rrDescs = new HashMap<>();
		for( Entry< String, OperationDeclaration > entry : n.operationsMap().entrySet() ) {
			if( entry.getValue() instanceof OneWayOperationDeclaration ) {
				oneWayDescs.put( entry.getKey(),
					buildOneWayTypeDescription( (OneWayOperationDeclaration) entry.getValue() ) );
			} else { // Request-Response
				rrDescs.put( entry.getKey(),
					buildRequestResponseTypeDescription( (RequestResponseOperationDeclaration) entry.getValue() ) );
			}
		}

		InterfaceExtender extender = new InterfaceExtender(
			oneWayDescs,
			rrDescs,
			buildOneWayTypeDescription( n.defaultOneWayOperation() ),
			buildRequestResponseTypeDescription( n.defaultRequestResponseOperation() ) );

		interfaceExtenders.put( n.name(), extender );
	}

	public void visit( CourierDefinitionNode n ) {
		if( firstPass ) {
			visitLater( n );
		} else {
			currCourierInputPort = inputPorts.get( n.inputPortName() );
			if( currCourierInputPort == null ) {
				error( n.context(), "cannot find input port: " + n.inputPortName() );
			} else {
				n.body().accept( this );
				if( currCourierInputPort.location().toString().equals( Constants.LOCAL_LOCATION_KEYWORD ) ) {
					interpreter.commCore().localListener().inputPort().aggregationMap()
						.putAll( currCourierInputPort.aggregationMap() );
				}
			}
			currCourierInputPort = null;
		}
	}

	private OneWayOperation getExtendedOneWayOperation( String inputPortName, String operationName ) {
		AggregationConfiguration conf = getAggregationConfiguration( inputPortName, operationName );
		OneWayTypeDescription desc = conf.aggregatedInterface.oneWayOperations().get( operationName );
		Type extenderType = null;
		if( conf.interfaceExtender != null ) {
			OneWayTypeDescription extenderDesc = conf.interfaceExtender.getOneWayTypeDescription( operationName );
			if( extenderDesc != null ) {
				extenderType = extenderDesc.requestType();
			}
		}

		return new OneWayOperation(
			operationName,
			(extenderType == null) ? desc.requestType() : Type.extend( desc.requestType(), extenderType ) );
	}

	private RequestResponseOperation getExtendedRequestResponseOperation( String inputPortName, String operationName ) {
		AggregationConfiguration conf = getAggregationConfiguration( inputPortName, operationName );
		RequestResponseTypeDescription desc = conf.aggregatedInterface.requestResponseOperations().get( operationName );

		Map< String, Type > extendedFaultMap = new HashMap<>();
		extendedFaultMap.putAll( desc.faults() );

		Type requestExtenderType = null;
		Type responseExtenderType = null;

		if( conf.interfaceExtender != null ) {
			RequestResponseTypeDescription extenderDesc =
				conf.interfaceExtender.getRequestResponseTypeDescription( operationName );
			if( extenderDesc != null ) {
				requestExtenderType = extenderDesc.requestType();
				responseExtenderType = extenderDesc.responseType();
				extendedFaultMap.putAll( extenderDesc.faults() );
			}
		}

		return new RequestResponseOperation(
			operationName,
			new RequestResponseTypeDescription(
				(requestExtenderType == null) ? desc.requestType()
					: Type.extend( desc.requestType(), requestExtenderType ),
				(responseExtenderType == null) ? desc.responseType()
					: Type.extend( desc.responseType(), responseExtenderType ),
				extendedFaultMap ) );
	}

	public void visit( CourierChoiceStatement n ) {
		for( CourierChoiceStatement.InterfaceOneWayBranch branch : n.interfaceOneWayBranches() ) {
			for( Map.Entry< String, OperationDeclaration > entry : branch.interfaceDefinition.operationsMap()
				.entrySet() ) {
				if( entry.getValue() instanceof OneWayOperationDeclaration ) {
					currCourierOperationName = entry.getKey();
					currCourierInputPort.aggregationMap().put(
						entry.getKey(),
						AggregatedOperation.createWithCourier(
							getExtendedOneWayOperation( currCourierInputPort.name(), currCourierOperationName ),
							buildVariablePath( branch.inputVariablePath ),
							buildProcess( branch.body ),
							n.context() ) );
				}
			}
		}

		for( CourierChoiceStatement.InterfaceRequestResponseBranch branch : n.interfaceRequestResponseBranches() ) {
			for( Map.Entry< String, OperationDeclaration > entry : branch.interfaceDefinition.operationsMap()
				.entrySet() ) {
				if( entry.getValue() instanceof RequestResponseOperationDeclaration ) {
					currCourierOperationName = entry.getKey();
					currCourierInputPort.aggregationMap().put(
						entry.getKey(),
						AggregatedOperation.createWithCourier(
							getExtendedRequestResponseOperation( currCourierInputPort.name(),
								currCourierOperationName ),
							buildVariablePath( branch.inputVariablePath ),
							buildVariablePath( branch.outputVariablePath ),
							buildProcess( branch.body ),
							n.context() ) );
				}
			}
		}

		for( CourierChoiceStatement.OperationOneWayBranch branch : n.operationOneWayBranches() ) {
			currCourierOperationName = branch.operation;
			currCourierInputPort.aggregationMap().put(
				branch.operation,
				AggregatedOperation.createWithCourier(
					getExtendedOneWayOperation( currCourierInputPort.name(), currCourierOperationName ),
					buildVariablePath( branch.inputVariablePath ),
					buildProcess( branch.body ),
					n.context() ) );
		}

		for( CourierChoiceStatement.OperationRequestResponseBranch branch : n.operationRequestResponseBranches() ) {
			currCourierOperationName = branch.operation;
			currCourierInputPort.aggregationMap().put(
				branch.operation,
				AggregatedOperation.createWithCourier(
					getExtendedRequestResponseOperation( currCourierInputPort.name(), currCourierOperationName ),
					buildVariablePath( branch.inputVariablePath ), buildVariablePath( branch.outputVariablePath ),
					buildProcess( branch.body ),
					n.context() ) );
		}

		currCourierOperationName = null;
	}

	public void visit( NotificationForwardStatement n ) {
		AggregationConfiguration conf =
			getAggregationConfiguration( currCourierInputPort.name(), currCourierOperationName );
		try {
			OutputPort outputPort;
			if( n.outputPortName() != null ) {
				outputPort = interpreter.getOutputPort( n.outputPortName() );
			} else {
				outputPort = conf.defaultOutputPort;
			}
			currProcess = new ForwardNotificationProcess(
				currCourierOperationName,
				outputPort,
				buildVariablePath( n.outputVariablePath() ),
				conf.aggregatedInterface.oneWayOperations().get( currCourierOperationName ),
				(conf.interfaceExtender == null) ? null
					: conf.interfaceExtender.getOneWayTypeDescription( currCourierOperationName ),
				n.context() );
		} catch( InvalidIdException e ) {
			error( n.context(), e );
		}
	}

	public void visit( ProvideUntilStatement n ) {
		currProcess = new ProvideUntilProcess( (NDChoiceProcess) buildProcess( n.provide() ),
			(NDChoiceProcess) buildProcess( n.until() ) );
	}

	@Override
	public void visit( TypeChoiceDefinition n ) {
		final boolean wasInsideType = insideType;
		insideType = true;

		currType = Type.createChoice( n.cardinality(), buildType( n.left() ), buildType( n.right() ) );

		insideType = wasInsideType;
		if( insideType == false && insideOperationDeclarationOrInstanceOf == false ) {
			typeMap.put( n, currType );
		}
	}

	public void visit( SolicitResponseForwardStatement n ) {
		AggregationConfiguration conf =
			getAggregationConfiguration( currCourierInputPort.name(), currCourierOperationName );
		try {
			OutputPort outputPort;
			if( n.outputPortName() != null ) {
				outputPort = interpreter.getOutputPort( n.outputPortName() );
			} else {
				outputPort = conf.defaultOutputPort;
			}
			currProcess = new ForwardSolicitResponseProcess(
				currCourierOperationName,
				outputPort,
				buildVariablePath( n.outputVariablePath() ),
				buildVariablePath( n.inputVariablePath() ),
				conf.aggregatedInterface.requestResponseOperations().get( currCourierOperationName ),
				(conf.interfaceExtender == null) ? null
					: conf.interfaceExtender.getRequestResponseTypeDescription( currCourierOperationName ),
				n.context() );
		} catch( InvalidIdException e ) {
			error( n.context(), e );
		}
	}

	@Override
	public void visit( ImportStatement n ) {

	}

	@Override
	public void visit( ServiceNode n ) {
		services.put( n.name(), n );
	}

	@Override
	public void visit( EmbedServiceNode n ) {
		try {
			final VariablePath path =
				n.bindingPort() == null ? null
					: interpreter.getOutputPort( n.bindingPort().id() ).locationVariablePath();

			Expression passingArgument = buildExpression( n.passingParameter() );

			final EmbeddedServiceConfiguration embeddedServiceConfiguration =
				new EmbeddedServiceLoader.ServiceNodeEmbeddedConfiguration( n.service().type(), n.service(),
					passingArgument );

			interpreter.addEmbeddedServiceLoader(
				EmbeddedServiceLoader.create(
					interpreter,
					embeddedServiceConfiguration,
					path ) );
		} catch( EmbeddedServiceLoaderCreationException e ) {
			error( n.context(), e );
		} catch( InvalidIdException e ) {
			error( n.context(), "could not find port " + n.bindingPort().id() );
		}
	}

	public void visit( SolicitResponseExpressionNode n ) {
		try {
			currExpression =
				new SolicitResponseExpression(
					n.id(),
					interpreter.getOutputPort( n.outputPortId() ),
					buildExpression( n.outputExpression() ),
					solicitResponseTypes.get( n.outputPortId() ).get( n.id() ),
					n.context() );
		} catch( InvalidIdException e ) {
			error( n.context(), e );
		}
	}
}
