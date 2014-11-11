package com.knockchat.utils.settings;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class ParseErrEx extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public ParseErrEx(String txt){
		super(txt);
	}

	public ParseErrEx(Exception e) {
		super(e);
	}

}
