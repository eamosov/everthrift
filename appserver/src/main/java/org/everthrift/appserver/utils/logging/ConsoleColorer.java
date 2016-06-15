package org.everthrift.appserver.utils.logging;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class ConsoleColorer {
	
	public final static int RESET = 0;
	public final static int BRIGHT  = 1;
	public final static int DIM = 2;
	public final static int UNDERLINE  = 3;
	public final static int BLINK = 4;
	public final static int REVERSE = 7;
	public final static int HIDDEN = 8;

	public final static int BLACK  = 0;
	public final static int RED = 1;
	public final static int GREEN = 2;
	public final static int YELLOW = 3;
	public final static int BLUE = 4;
	public final static int MAGENTA = 5;
	public final static int CYAN = 6;
	public final static int WHITE = 7;

	public static String color(int attr, int fg, int bg) {	
		StringBuilder sb = new StringBuilder();
		sb.append(((char)0x1B));
		sb.append('[');
		sb.append(attr);
		sb.append(';');
		sb.append(30 + fg);
		sb.append(';');
		sb.append(40 + bg);
		sb.append("m");
		
		return sb.toString();
	}
	
	public static String color( int fg ) {	
		StringBuilder sb = new StringBuilder();
		sb.append(((char)0x1B));
		sb.append('[');
		sb.append( 0 );
		sb.append(';');
		sb.append(30 + fg);
		sb.append("m");
		
		return sb.toString();
	}

	public static String restore() {
		StringBuilder sb = new StringBuilder();
		sb.append(((char)0x1B));
		sb.append("[00m");
		
		return sb.toString();
	}
}

