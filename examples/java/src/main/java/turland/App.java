package turland;

import org.openjdk.jmh.annotations.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.ByteArrayOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.lang.Integer;
import java.util.Random;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

import org.apache.nifi.processors.thrift.FlowFileRequest;
import org.apache.nifi.processors.thrift.FlowFileReply;
import org.apache.nifi.processors.thrift.ThriftFlowFile;

// https://hc.apache.org/httpcomponents-client-5.2.x/
// https://github.com/apache/httpcomponents-client/tree/master/httpclient5/src/test/java/org/apache/hc/client5/http/examples
//import org.apache.nifi.processors.FromThriftProcessor;

//@State(Scope.Benchmark)
//class ExecutionPlan {
//
//    @Param({ "100", "200", "300", "500", "1000" })
//    public int iterations;
//
//    @Setup(Level.Invocation)
//    public void setUp() {
//
//    }
//}

/**
 * mvn exec:java -Dexec.mainClass="turland.App" -Dexec.args="-v=1234"
 */
@State(Scope.Benchmark)
public class App {
// http://javadox.com/org.openjdk.jmh/jmh-core/0.9/org/openjdk/jmh/annotations/package-summary.html
    @Param({ "1", "5"})
    private int iterations;

    @Param({"1","10", "512", "1024", "2048", "5120"})
    private int contentSize;

    @Fork(value = 5)
    @Benchmark
    @Measurement(iterations=2,time = 2)
    @Warmup(iterations=1,time = 1)
    @Threads(value = 2)
    public void request_via_param() {
      this.request_many(this.iterations,this.contentSize);
    }

    public void request_many(int iterations,int contentSize) {
        FlowFileRequest ffr = new FlowFileRequest(1L,new ThriftFlowFile());
        ffr.setId(63);

        ffr.getFlowFile().putToAttributes("wibble_key", "wibble_value");
        //FlowFileRequest.metaDataMap.forEach((key, fieldMetaData) -> System.err.println(key + " : " + fieldMetaData.fieldName + " type " + fieldMetaData.valueMetaData.type));
        //System.exit(1);
        //String mime = "I am some mime";
        //int byte_size = mime.getBytes().length;
        //ffr.setContent(mime.getBytes());
        Random random = new Random();
        byte[] b = new byte[contentSize * 1000];
        random.nextBytes(b);
        ffr.getFlowFile().setContent(b);

        //System.out.println("request_many "+ iterations);
        try {
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            byte[] serializedFlowFileRequest = serializer.serialize(ffr);
            try {
                FlowFileReply flowFileReply = new FlowFileReply();
                URL url = new URL("http://127.0.0.1:9090/");

                for (int i = 0; i <= iterations; i++) {
                    HttpURLConnection con = (HttpURLConnection) url.openConnection();
                    con.setRequestMethod("PUT");
                    con.setRequestProperty("Content-Type", "application/json; utf-8");
                    con.setDoOutput(true);
                    OutputStream os = con.getOutputStream();

                    os.write(serializedFlowFileRequest, 0, serializedFlowFileRequest.length);
                    // BufferedReader br = new BufferedReader(new
                    // InputStreamReader(con.getInputStream(), "utf-8"));
                    InputStream in = con.getInputStream();

                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                    int nRead;
                    byte[] data = new byte[1024];
                    while ((nRead = in.read(data, 0, data.length)) != -1) {
                        buffer.write(data, 0, nRead);
                    }
                    buffer.flush();
                    byte[] bytes = buffer.toByteArray();
                    deserializer.deserialize(flowFileReply, bytes);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Error: ============================= " + e);
            }


        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error: ============================= " + e);
        }

    }

    //@Fork(value = 0)
    //@Benchmark
    //@Measurement(iterations=2,time = 2)
    //@Warmup(iterations=1,time = 1)
    //@Threads(value = 1)
    public void request() {
        // System.out.println("Hello World!");
        FlowFileRequest ffr = new FlowFileRequest(63L,new ThriftFlowFile());
        String mime = "I am some mime";
        // byte[] content = new byte[1000];
        ffr.getFlowFile().putToAttributes("wibble_key", "wibble_value");
        int byte_size = mime.getBytes().length;
        ffr.getFlowFile().setContent(mime.getBytes());
        FlowFileReply flowFileReply = new FlowFileReply();
        try {
           TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            byte[] serializedFlowFileRequest = serializer.serialize(ffr);
            URL url = new URL("http://127.0.0.1:9090/");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("PUT");
            con.setRequestProperty("Content-Type", "application/json; utf-8");
            con.setDoOutput(true);
            OutputStream os = con.getOutputStream();
            os.write(serializedFlowFileRequest, 0, serializedFlowFileRequest.length);
            // BufferedReader br = new BufferedReader(new
            // InputStreamReader(con.getInputStream(), "utf-8"));
            InputStream in = con.getInputStream();
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            // System.out.println("Received " + in.available() + " bytes");
            byte[] bytes = new byte[in.available()];
            // DataInputStream dataInputStream = new DataInputStream(new
            // FileInputStream(file));
            in.read(bytes);
            // byte[] bytes = is.ReadallbytesreadAllBytes();
            deserializer.deserialize(flowFileReply, bytes);
//
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error: ============================= " + e);
        }
    }

    public void request_puthrift() {
        // System.out.println("Hello World!");
        FlowFileRequest ffr = new FlowFileRequest(63L,new ThriftFlowFile());
        String mime = "I am some mime";
        // byte[] content = new byte[1000];
        ffr.getFlowFile().putToAttributes("wibble_key", "wibble_value");
        int byte_size = mime.getBytes().length;
        ffr.getFlowFile().setContent(mime.getBytes());
        FlowFileReply flowFileReply = new FlowFileReply();
        try {
           TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            URL url = new URL("http://127.0.0.1:9090/putthrift");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("User-Agent", "flooby_agent");
            int responseCode = con.getResponseCode();
            //System.out.println("GET Response Code :: " + responseCode);
            if (responseCode == HttpURLConnection.HTTP_OK) { // success
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        con.getInputStream()));
                String inputLine;
                StringBuffer response = new StringBuffer();

                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();

                // print result
                System.out.println(response.toString());
            } else {
                System.out.println("GET request not worked");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error: ============================= " + e);
        }
    }



    public void request_verbose() {
        FlowFileRequest ffr = new FlowFileRequest(63L,new ThriftFlowFile());
        String mime = "I am some mime";
        // byte[] content = new byte[1000];
        ffr.getFlowFile().putToAttributes("wibble_key", "wibble_value");
        int byte_size = mime.getBytes().length;
        ffr.getFlowFile().setContent(mime.getBytes());
        // assertTrue("bytes size > 1", 1 < byte_size);
        // byte[] serializedFlowFileRequest;
        // https://docs.oracle.com/javase/8/docs/api/index.html
        //System.out.println(" flowFileRequest " + ffr);
        FlowFileReply flowFileReply = new FlowFileReply();
        try {
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            byte[] serializedFlowFileRequest = serializer.serialize(ffr);
            URL url = new URL("http://127.0.0.1:9090/");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("PUT");
            con.setRequestProperty("Content-Type", "application/json; utf-8");
            con.setDoOutput(true);
            OutputStream os = con.getOutputStream();
            os.write(serializedFlowFileRequest, 0, serializedFlowFileRequest.length);
            // BufferedReader br = new BufferedReader(new
            // InputStreamReader(con.getInputStream(), "utf-8"));
            InputStream in = con.getInputStream();
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            System.out.println("Received " + in.available() + " bytes");
            byte[] bytes = new byte[in.available()];
            // DataInputStream dataInputStream = new DataInputStream(new
            // FileInputStream(file));
            in.read(bytes);

            // byte[] bytes = is.ReadallbytesreadAllBytes();
            deserializer.deserialize(flowFileReply, bytes);
            //System.out.println(" flowFileReply " + flowFileReply);
            for (Map.Entry<String, String> entry : flowFileReply.getFlowFile().getAttributes().entrySet()) {
                System.out.println(entry.getKey() + " : " + entry.getValue());
            }

            // InputStream content = new ByteArrayInputStream(serializer.serialize(ffr));
            // System.out.println("content has count =============================" +
            // content.available());
            // // Add the content to the runner
            // assertTrue("enqued content has a size", 1 < content.available());
//
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error: ============================= " + e);

        }

    }

    public static void main(String[] args) {
        Options options = new Options();
        {
            Option option = Option.builder("v")
            .required(true)
            .longOpt("value")
            .hasArg()
            .build();
            options.addOption(option);
        }
        {
            Option option = Option.builder("h")
            .longOpt("help")
            .build();
            options.addOption(option);
        }
        CommandLineParser parser = new DefaultParser();
        try{
            CommandLine cmd = parser.parse(options, args);
            if (cmd.hasOption("value")) {
                System.out.println(cmd.getOptionValue("value"));
            }
            if(cmd.hasOption("help")) {
                System.out.println("Don't eat yellow snow");
            }
        }
        catch(ParseException pe){
            System.out.println(pe);
        }

        FlowFileRequest ffr = new FlowFileRequest(63L,new ThriftFlowFile());
        FlowFileRequest.metaDataMap.forEach((key, value) -> System.err.println(key + " : " + value));
        //System.exit(0);
        App app = new App();
        //app.request_puthrift();
        app.request_verbose();
        //app.request_many(500,10);

    }
}
