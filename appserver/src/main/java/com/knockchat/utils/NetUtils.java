package com.knockchat.utils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class NetUtils {
	
	public static String getFirstPublicHostAddress(){
		final List<InetAddress> ret = getPublicAddresses();
		return ret.get(0).getHostAddress();
	}

	public static List<InetAddress> getPublicAddresses() {
		final List<InetAddress> ret = new ArrayList<InetAddress>();

		try {
			for (Enumeration<NetworkInterface> en = NetworkInterface
					.getNetworkInterfaces(); en.hasMoreElements();) {
				final NetworkInterface intf = en.nextElement();

				if (intf.isUp()) {

					for (Enumeration<InetAddress> enumIpAddr = intf
							.getInetAddresses(); enumIpAddr.hasMoreElements();) {
						final InetAddress addr = enumIpAddr.nextElement();
						if (addr instanceof Inet4Address
								&& !addr.isAnyLocalAddress()
								&& !addr.isLinkLocalAddress()
								&& !addr.isLoopbackAddress()
								&& !addr.isMulticastAddress())
							ret.add(addr);
					}
				}
			}
		} catch (SocketException e) {
		}
		
		return ret;
	}
	
	public static String localToPublic(String address){
		String host;
		try {
			final InetAddress i = InetAddress.getByName(address);
			if (i.isAnyLocalAddress()){
				host = NetUtils.getFirstPublicHostAddress();
			}else{
				host = i.getHostAddress();
			}
		} catch (UnknownHostException e) {
			host = address;
		}
		return host;
	}
}
