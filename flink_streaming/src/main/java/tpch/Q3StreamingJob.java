package tpch;

import tpch.model.*;
import tpch.process.Q3ProcessFunction;
import tpch.source.TPCHFIFOStreamingSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Q3StreamingJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        env.addSource(new TPCHFIFOStreamingSource())
                .keyBy(e -> 0L)
                .process(new Q3ProcessFunction())
                .print();


        env.execute("TPC-H Q3 FIFO Streaming");
    }
}
