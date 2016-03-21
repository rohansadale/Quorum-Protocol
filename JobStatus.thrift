include "node.thrift"
struct JobStatus
{
	1:bool status,
	2:string content,
	3:list<node.Node> path
}
