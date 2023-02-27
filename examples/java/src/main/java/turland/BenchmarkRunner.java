package turland;
import org.apache.nifi.processors.thrift.FlowFileRequest;
import org.apache.nifi.processors.thrift.FlowFileReply;
public class BenchmarkRunner {
	public static void main(String[] args) throws Exception {
        //FlowFileRequest ffr = new FlowFileRequest();
        //FlowFileRequest.metaDataMap.forEach((key, value) -> //System.err.println(key + " : " + value));
		org.openjdk.jmh.Main.main(args);
	}
}
