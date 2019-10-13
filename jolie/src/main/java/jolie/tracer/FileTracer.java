package jolie.tracer;

import jolie.Interpreter;
import jolie.runtime.Value;
import jolie.runtime.ValuePrettyPrinter;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Date;
import java.util.function.Supplier;

public class FileTracer implements Tracer {

    private int actionCounter = 0;
    Writer fileWriter;
    private final Interpreter interpreter;

    public FileTracer(Interpreter interpreter ) {
        this.interpreter = interpreter;
        String format = "ddMMyyyyHHmmss";
        SimpleDateFormat sdf = new SimpleDateFormat( format );
        final Date now = new Date();
        String filename = sdf.format(now);
        File logFile = new File( filename + ".jolie.log.json");
        try {
            fileWriter = new FileWriter( logFile, true );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void trace(Supplier<? extends TraceAction> supplier) {
        final TraceAction action = supplier.get();
        actionCounter++;
        if ( action instanceof MessageTraceAction ) {
            trace( (MessageTraceAction) action );
        } else if ( action instanceof EmbeddingTraceAction ) {
            trace( (EmbeddingTraceAction) action );
        }
    }

    private String getCurrentTimeStamp() {
        String format = "dd/MM/yyyy HH:mm:ss.SSS";
        SimpleDateFormat sdf = new SimpleDateFormat( format );
        final Date now = new Date();
        return sdf.format(now);
    }

    private void trace( EmbeddingTraceAction action )
    {
        StringBuilder stBuilder = new StringBuilder();
        stBuilder.append( "{");
        stBuilder.append( "\"").append( Integer.toString( actionCounter ) ).append( "\":[" );
        stBuilder.append("\"").append( getCurrentTimeStamp() ).append("\",");
        stBuilder.append( "\"").append( interpreter.logPrefix() ).append( "\"," );

        switch( action.type() ) {
            case SERVICE_LOAD:
                stBuilder.append( "\"").append( "emb" ).append( "\"," );
                break;
            default:
                break;
        }
        stBuilder.append( "\"").append( action.name() ).append( "\"," );
        stBuilder.append( "\"").append( action.description() ).append( "\"]}\n" );
        try {
            fileWriter.write(stBuilder.toString());
            fileWriter.flush();
        } catch( IOException e ) {
            e.printStackTrace();
        }


    }

    private void trace( MessageTraceAction action )
    {
        StringBuilder stBuilder = new StringBuilder();
        stBuilder.append( "{");
        stBuilder.append( "\"").append( Integer.toString( actionCounter ) ).append( "\":[" );
        stBuilder.append("\"").append( getCurrentTimeStamp() ).append("\",");

        stBuilder.append( "\"").append( interpreter.logPrefix() ).append( "\"," );
        switch( action.type() ) {
            case SOLICIT_RESPONSE:
                stBuilder.append( "\"").append( "sr" ).append( "\"," );
                break;
            case NOTIFICATION:
                stBuilder.append( "\"").append( "n" ).append( "\"," );
                break;
            case ONE_WAY:
                stBuilder.append( "\"").append( "ow" ).append( "\"," );
                break;
            case REQUEST_RESPONSE:
                stBuilder.append( "\"").append( "rr" ).append( "\"," );
                break;
            case COURIER_NOTIFICATION:
                stBuilder.append( "\"").append( "cn" ).append( "\"," );
                break;
            case COURIER_SOLICIT_RESPONSE:
                stBuilder.append( "\"").append( "csr" ).append( "\"," );
                break;
            default:
                break;
        }
        stBuilder.append( "\"").append( action.description() ).append( "\"," );

        stBuilder.append( "\"").append( action.name() ).append( "\"" );
        if ( action.message() != null ) {
            stBuilder.append( ",\"").append( action.message().id() ).append( "\"," );

            Writer writer = new StringWriter();
            Value messageValue = action.message().value();
            if ( action.message().isFault() ) {
                messageValue = action.message().fault().value();
            }
            ValuePrettyPrinter printer = new ValuePrettyPrinter(
                    messageValue,
                    writer,
                    "Value:"
            );
            printer.setByteTruncation( 50 );
            printer.setIndentationOffset( 6 );
            try {
                printer.run();
            } catch( IOException e ) {} // Should never happen

            String encodedString = Base64.getEncoder().encodeToString(writer.toString().trim().getBytes());
            stBuilder.append( "\"").append( encodedString ).append( "\"" );
        }
        stBuilder.append("]}\n");
        try {
            fileWriter.write(stBuilder.toString());
            fileWriter.flush();
        } catch( IOException e ) {
            e.printStackTrace();
        }
    }
}