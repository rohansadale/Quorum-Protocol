include "node.thrift"
include "job.thrift"
include "JobStatus.thrift"
service QuorumService
{
	list<node.Node> join(1:node.Node node),
	node.Node GetNode()
	string read(1:string filename,2:string directory),
    bool write(1:string filename,2:string directory,3:string content),
    string version(1:string filename,2:string directory),
	JobStatus.JobStatus submitJob(1:job.Job job)
}
