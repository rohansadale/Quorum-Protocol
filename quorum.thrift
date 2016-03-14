include "node.thrift"
service QuorumService
{
	list<node.Node> join(1:node.Node node),
	string read(1:string filename,2:string directory,3:bool shouldRead),
	bool write(1:string filename,2:string directory,3:string content),
	list<node.Node> getNodes(1:i32 numNodes),
	node.Node GetNode(),
	bool update(1:list<node.Node> neigh)
}
