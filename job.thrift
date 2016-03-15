include "node.thrift"
struct Job
{
	1: node.Node node,
	2: i32 id,
	3: i32 optype,
	4: string filename,
	5: string content
}
