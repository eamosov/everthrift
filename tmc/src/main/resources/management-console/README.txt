Terracotta Management Console

This document contains important information you need to set up and run the Terracotta Management Console. Be sure to read this entire document. 


-----------
I. Introduction

The Terracotta Management Console (TMC) is a web-based administration and monitoring application with a wealth of advantages, including:

* Feature-rich and easy-to-use in-browser interface
* Remote management capabilities requiring only a web browser and network connection
* Cross-platform deployment
* Available SSL-based secure connections
* Choice of local (built-in), LDAP or Active Directory authentication and authorization
* Fine-grained statistics from individual components including servers, applications, caches
* Aggregate statistics from multiple nodes
* Flexible deployment model plugs into both development environments and secure production architectures

The TMC is served by the Terracotta Management Server (TMS), which is included in Terracotta BigMemory kits under the management-console directory.

-----------
II. Installation

Before connecting to the TMC, you must run the TMS. The files needed to run the TMS are found under the kit's /management-console directory with this README file. The TMS can be run directly with the provided container (see below). To run it with an application server of your choice, use the file /management-console/webapps/tmc.war. Follow the specifications and requirements of your chosen application server for deploying a WAR-based application.

A Terracotta license file must be in the Terracotta root installation directory.


-----------
III. Configuration
If you are using a cluster, read the section on "BigMemory Max Direct Connection." If you are using a standalone node, go to "BigMemory Go REST Service Connection” below.

BigMemory Max Direct Connection
A Terracotta BigMemory Max cluster can be managed directly by connecting the TMC to any one of the servers in the cluster. All other servers and clients become visible to the TMC through that initial connection. No special setup or configuration is required. Create a new connection and enter the URI to a server in the form <scheme>://<host-address>:<tsa-port>. For example, to connect to myServer.com over an unsecured connection and using the default TSA port, use http://myServer.com:9510.

BigMemory Go REST Service Connection
To manage a client or standalone node (Terracotta Ehcache client or BigMemory Go) using the TMC, you must enable the REST management service on that node. 

To enable the REST management service on a BigMemory Go or Ehcache node, set the following element in ehcache.xml:

   <ehcache ... >
   ...
    <managementRESTService enabled="true" bind="<ip_address>:<port>"/>
   ...
   </ehcache>

where <ip_address> is the local network interface's IP address and <port> is the port number used to manage the node. The following defaults are in effect for <managementRESTService>:

   * enabled="false"
   * bind="0.0.0.0:9888"
     This IP address binds the specified port all network interfaces on the local node.

The TMC can connect to the standalone node using <scheme>://<host-address>:<bind-port>. <host-address> must be (or must resolve to) the IP address specified in the bind attribute. For example, to connect to myClient.com over an unsecured connection and using the port 9887, use http://myServer.com:9887.

-----------
IV. Setting Up Security

Available types of security:

* Built-in identity assertion
* File-based and LDAP-based user authentication and authorization
* SSL-based encryption

These security types can be used in tandem with security mechanisms set up on BigMemory Go and BigMemory Max.

When you first connect to the TMC, a security-setup screen appears. On-screen instructions provide guidance for setting up security for the TMS and TMC. Note: If the TMC appears without the security setup screen, check that a valid Terracotta license file is present in the Terracotta root installation directory.

To learn more about setting up security, see the online documentation at http://www.terracotta.org/kit/reflector?kitID=4.1&pageID=TMSsecurity.


-----------
V. Starting and Connecting to the TMC

Start the TMC by running the following script:

   management-console/bin/start-tmc.sh

To stop the TMC, use the following script:

   management-console/bin/stop-tmc.sh

For Microsoft Windows, use start.bat and stop.bat, available in the same directory.

Connect to the TMC using the following URI with a standard web browser:

   http://localhost:9889/tmc

If you are connecting remotely, substitute the appropriate hostname. If you have set up secure browser connections, use "https:" instead of "http:".


-----------
VI. Using the TMC

When you first connect to the TMC, the security setup page appears, where you can choose to run the TMC with or without authentication. Authentication can also be enabled and disabled in the TMC Settings panel once the TMC is running.

If you do not enable authentication (or if a valid Terracotta license file is not found), you will be directly connected to the TMC without being prompted for security setup. Note that with authentication disabled, the TMC cannot connect to secured nodes.

For more information on using the TMC, click the Help links available on certain pages or choose Help from the tool bar to access the TMC online help.
