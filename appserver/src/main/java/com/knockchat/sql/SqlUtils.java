package com.knockchat.sql;

import gnu.trove.map.TLongLongMap;
import gnu.trove.procedure.TLongLongProcedure;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class SqlUtils {

	@SuppressWarnings("unchecked")
	public static Object toSqlParam( Object param ) {
		if ( param == null )
			return null;

		if ( param.getClass().isArray() ) {

			if ( param instanceof int[] ) {
				return toSqlParam( (int[]) param );
			} else if ( param instanceof long[] ) {
				return toSqlParam( (long[]) param );
			} else if ( param instanceof short[] ) {
				return toSqlParam( (short[]) param );
			} else if ( param instanceof byte[] ) {
				return toSqlParam( param );
			} else if ( param instanceof Object[] ) {
				return toSqlParam( (Object[]) param );
			} else {
				throw new RuntimeException( "Unsupported array type" );
			}

		}else if ( param instanceof Map){
			return toSqlParam( (Map<?,?>) param );
		}else if ( param instanceof TBase ){
			final TMemoryBuffer tr = new TMemoryBuffer(1024);
			final TJSONProtocol pr = new TJSONProtocol(tr);
					
			try {
				((TBase)param).write(pr);
				return tr.toString("UTF-8");
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
		}else if ( param instanceof Iterable ) {
			return toSqlParam( (Iterable<?>) param );
		}

		return param;
	}
	
	public static String toSqlParam( TLongLongMap hstore ) {
		
		if (hstore == null)
			return null;

		final StringBuilder buf = new StringBuilder();
		final AtomicBoolean needComa = new AtomicBoolean(false);
		
		hstore.forEachEntry(new TLongLongProcedure(){

			@Override
			public boolean execute(long a, long b) {
				
				if (needComa.getAndSet(true))
					buf.append(",");

				buf.append("\"");
				buf.append(a);
				buf.append("\"=>\"");
				buf.append(b);
				buf.append("\"");
				
				return true;
			}});

		return buf.toString();
	}
	
	public static Object toSqlParam( Map<?,?> hstore ) {
		
		final StringBuilder b = new StringBuilder();
		boolean needComa = false;
		
		for (Entry<?,?> e: hstore.entrySet()){
			if (needComa)
				b.append(",");
			
			b.append("\"");
			b.append(toSqlParam(e.getKey()).toString().replaceAll("\"", "\\\\\""));
			b.append("\"");
			b.append("=>");
			
			if (e.getValue() !=null){
				b.append("\"");
				b.append(toSqlParam(e.getValue()).toString().replaceAll("\"", "\\\\\""));
				b.append("\"");				
			}else{
				b.append("NULL");
			}
			needComa = true;
		}
		return b.toString();
	}

	public static Object toSqlParam( int[] arr ) {
		if ( arr == null )
			return null;

		int l = arr.length;

		StringBuilder b = new StringBuilder();
		b.append( '{' );
		b.ensureCapacity( l * 2 + 2 );

		if ( l > 0 ) {
			b.append( arr[0] );
			for ( int i = 1; i < l; ++i ) {
				b.append( ',' );
				b.append( arr[i] );
			}

		}

		b.append( '}' );

		return b.toString();
	}

	public static Object toSqlParam( long[] arr ) {
		if ( arr == null )
			return null;

		int l = arr.length;

		StringBuilder b = new StringBuilder();
		b.append( '{' );
		b.ensureCapacity( l * 2 + 2 );

		if ( l > 0 ) {
			b.append( arr[0] );
			for ( int i = 1; i < l; ++i ) {
				b.append( ',' );
				b.append( arr[i] );
			}

		}

		b.append( '}' );

		return b.toString();
	}

	public static Object toSqlParam( short[] arr ) {
		if ( arr == null )
			return null;

		int l = arr.length;

		StringBuilder b = new StringBuilder();
		b.append( '{' );
		b.ensureCapacity( l * 2 + 2 );

		if ( l > 0 ) {
			b.append( arr[0] );
			for ( int i = 1; i < l; ++i ) {
				b.append( ',' );
				b.append( arr[i] );
			}

		}

		b.append( '}' );

		return b.toString();
	}

	public static Object toSqlParam( Object[] arr ) {
		if ( arr == null )
			return null;

		int l = arr.length;

		StringBuilder b = new StringBuilder();
		b.append( '{' );
		b.ensureCapacity( l * 2 + 2 );

		if ( l > 0 ) {
			b.append( arr[0] );
			for ( int i = 1; i < l; ++i ) {
				b.append( ',' );
				b.append( arr[i] );
			}

		}

		b.append( '}' );

		return b.toString();
	}

	public static Object toSqlParam( Iterable<?> coll ) {
		if ( coll == null )
			return null;

		StringBuilder b = new StringBuilder();
		b.append( '{' );

		Iterator<?> it = coll.iterator();

		if ( it.hasNext() ) {
			b.append( it.next() );
			while ( it.hasNext() ) {
				b.append( ',' );
				b.append( it.next() );
			}

		}

		b.append( '}' );

		return b.toString();
	}
}
