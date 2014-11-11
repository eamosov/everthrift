package com.knockchat.utils.settings;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class AllreadyExistEx extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public Object o;

	public AllreadyExistEx(String txt, Object o){
		super(txt);
		this.o = o;
	}

}
