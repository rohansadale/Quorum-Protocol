include "node.thrift"
service QuorumService
{
	bool join(1:node.Node node),
	string read(1:string filename,2:bool shouldRead),
	bool write(1:string filename,2:string content),
	list<node.Node> getNodes(1:i32 numNodes),
	node.Node GetNode()
}
