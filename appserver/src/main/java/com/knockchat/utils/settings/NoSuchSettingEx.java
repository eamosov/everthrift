package com.knockchat.utils.settings;

/**
 * 
 * @author efreet (Amosov Evgeniy)
 *
 */
public class NoSuchSettingEx extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public NoSuchSettingEx(String txt){
		super(txt);
	}

}
