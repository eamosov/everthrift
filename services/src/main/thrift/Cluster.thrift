namespace java com.knockchat.appserver.thrift.cluster
namespace php com.knockchat.thrift.cluster

struct NodeAddress{
	1:string host
	2:i32 port
}

struct NodeControllers{
	1:string version
	2:NodeAddress address
	3:list<string> externalControllers
}

struct NodeList{
	1:list<string> hosts;
	2:list<i32> ports;
	3:i32 hash;
}

struct VersionnedService {
	1:map<string,NodeList> versions // version -> NodeList
}

struct Node{
	1:list<NodeControllers> controllers
	2:string name
	3:NodeAddress jmxmp	
	4:NodeAddress http
}

struct ClusterConfiguration{
	1:map<string, VersionnedService> services // ServiceName:MethodName -> VersionnedService
	2:list<Node> nodes		//без Node::controllers
}

exception ClusterException{
}

service ClusterService{
	Node getNodeConfiguration();
	ClusterConfiguration getClusterConfiguration() throws (1:ClusterException clusterException)
	string getClusterConfigurationJSON() throws (1:ClusterException clusterException)
}
