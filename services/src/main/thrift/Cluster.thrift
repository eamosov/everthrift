namespace java com.knockchat.appserver.thrift.cluster
namespace php com.knockchat.thrift.cluster

struct Node{
	1:list<string> controllers
}

exception ClusterException{
}

service ClusterService{
	Node getNodeConfiguration();
	
	void onNodeConfiguration(1:Node node);	
}
