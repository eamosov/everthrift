namespace java org.everthrift.services.thrift.cluster
namespace php org.everthrift.services.thrift.cluster

struct Node{
	1:list<string> controllers
}

exception ClusterException{
}

service ClusterService{
	Node getNodeConfiguration();
	
	void onNodeConfiguration(1:Node node);	
}
