package org.weibo.analysis;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.Slide;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.weibo.analysis.entity.ControlMessage;
import org.weibo.analysis.entity.RelationLabel;
import org.weibo.analysis.entity.Vertex;
import org.weibo.analysis.graph.Edge;
import org.weibo.analysis.graph.GraphContainer;
import org.weibo.analysis.graph.TupleEdge;
import org.weibo.analysis.hash.HashPartition;
import org.weibo.analysis.network.Server;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class StreamingJob {
    private final transient static GraphContainer graphContainer = new GraphContainer();
    // a map descriptor to store the name of the rule (string) and the rule itself.
    private final transient static MapStateDescriptor<String, ControlMessage> controlMessageDescriptor = new MapStateDescriptor<>(
            RelationLabel.Control.getLabel(),
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<ControlMessage>() {
            }));
    private static boolean loop = true;
    private static String defaultWindowSize = "30.seconds";
    private static String defaultSlideSize = "10.seconds";

    public static void run() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.31.132.5:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "weibo");

        ControlMessage controlMessage = ControlMessage.buildDefault(defaultWindowSize, defaultSlideSize);

        graphContainer.setVersion(String.format("%s-%s-%s-%s-%s",
                controlMessage.getSlideSize(),
                controlMessage.getWindowSize(),
                controlMessage.getVertexLabel(),
                controlMessage.getEdgeLabel(),
                controlMessage.isWithGrouping() ? "Grouping" : "NoGrouping")
        );
        // a map descriptor to store the name of the rule (string) and the rule itself.
        final MapStateDescriptor<String, ControlMessage> controlMessageDescriptor = new MapStateDescriptor<>(
                RelationLabel.Control.getLabel(),
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<ControlMessage>() {
                }));

        // create a StreamEnviroment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        // register kafka as consumer to consume topic: weibo
        final FlinkKafkaConsumer<String> kafkaWeiboDataConsumer = new FlinkKafkaConsumer<>("weibo", new SimpleStringSchema(), properties);
        final DataStreamSource<String> kafkaWeiDataStringStreamSource = env.addSource(kafkaWeiboDataConsumer).setParallelism(1);

        // create a TableEnvironment for specific planner streaming
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // register kafka as consumer to consume topic: control as broadcast stream
        final FlinkKafkaConsumer<String> kafkaControlSignalConsumer = new FlinkKafkaConsumer<>("control", new SimpleStringSchema(), properties);
        final DataStream<String> broadcastStream = env.addSource(kafkaControlSignalConsumer).setParallelism(1);
        // broadcast the rules(ControlMessage) and create the broadcast state
        final BroadcastStream<ControlMessage> broadcastControlSignalStream = broadcastStream
                .map(new MapFunction<String, ControlMessage>() {
                    @Override
                    public ControlMessage map(String value) {
                        return ControlMessage.buildFromString(value);
                    }
                })
                .broadcast(controlMessageDescriptor);
        // this loop is used to reinitialize the stream with new properties when the stream crash on a certain Exception
        // that we induce from the generator application, we found this way for the current homework is best to avoid
        // arbitrary killing the processes since Flink stream doesn't support any lifecycle action yet and suns forever
        // with no possibility to restart or stop.
        while (loop) {
            try {
                loop = false;

                DataStream<Edge<Vertex, Vertex>> kafkaWeiboDataStreamDataSet = kafkaWeiDataStringStreamSource
                        .map(new MapFunction<String, Edge<Vertex, Vertex>>() {
                            @Override
                            //Deserialisation into Edge<Vertex,Vertex>
                            public Edge<Vertex, Vertex> map(String value) throws Exception {
                                Edge<String, String> edge = JSON.parseObject(value, new TypeReference<Edge<String, String>>(Edge.class) {
                                });
                                return Edge.build(edge);
                            }
                        })
                        .setParallelism(1)
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Edge<Vertex, Vertex>>() {
                            @Override
                            public long extractAscendingTimestamp(Edge<Vertex, Vertex> element) {
                                return element.getTimestamp();
                                // Consistent Hashing with id
                            }

                        }).setParallelism(1).partitionCustom(new HashPartition(), "id");

                kafkaWeiboDataStreamDataSet.print().setParallelism(1);
                // grouping or not
                if (controlMessage.isWithGrouping()) {
                    processWithGrouping(kafkaWeiboDataStreamDataSet, tableEnv, broadcastControlSignalStream, controlMessage);
                } else {
                    processWithoutGrouping(kafkaWeiboDataStreamDataSet, broadcastControlSignalStream, controlMessage);
                }
                env.execute("Weibo Data Streaming To Social Graph");
            } catch (JobExecutionException e) {
                System.out.println("catch exception");
                // update the ControlMessage value
                if ((e.getCause() != null && e.getCause() instanceof ControlMessageTriggeredException) ||
                        (e.getCause() != null && e.getCause().getCause() != null && e.getCause().getCause() instanceof ControlMessageTriggeredException)) {
                    ControlMessage newControlMessage;
                    if (e.getCause() instanceof ControlMessageTriggeredException) {
                        newControlMessage = ControlMessage.buildFromString(e.getCause().getMessage());
                    } else {
                        newControlMessage = ControlMessage.buildFromString(e.getCause().getCause().getMessage());
                    }

                    // Keep window size and slide size to default value  when current control message's value is null
                    if (Objects.isNull(newControlMessage.getWindowSize()))
                        controlMessage.setWindowSize(defaultWindowSize);
                    else
                        controlMessage.setWindowSize(newControlMessage.getWindowSize());

                    if (Objects.isNull(newControlMessage.getSlideSize())) {
                        controlMessage.setSlideSize(defaultSlideSize);
                    } else {
                        controlMessage.setSlideSize(newControlMessage.getSlideSize());
                    }


                    controlMessage.setWithGrouping(newControlMessage.isWithGrouping());
                    controlMessage.setVertexLabel(newControlMessage.getVertexLabel());
                    controlMessage.setEdgeLabel(newControlMessage.getEdgeLabel());

                    System.out.format("Restart with Control Message: %s", controlMessage.toString());
                    graphContainer.clear();
                    loop = true;
                    Thread.sleep(10 * 1000L);
                } else {
                    loop = true;
                    e.printStackTrace();
                    System.out.println("================= Connection Problem, Sleep then retry!=========================");
                    Thread.sleep(60 * 1000L);
                }
            } catch (Exception e) { //other exceptions, try connection again
                loop = true;
                e.printStackTrace();
                System.out.println("================= Connection Problem, Sleep then retry!=========================");
                Thread.sleep(60 * 1000L);
            }
        }


    }


    /**
     * @param kafkaStream            Consumes the kafkaStream and emits the output based on the windows/slider/vertex/edge to the web socket connected clients
     * @param broadcastControlStream broadcast stream from frontend ui by kafka control topic stream
     * @param controlMessage         current control message
     */
    private static void processWithoutGrouping(DataStream<Edge<Vertex, Vertex>> kafkaStream,
                                               BroadcastStream<ControlMessage> broadcastControlStream,
                                               ControlMessage controlMessage
    ) {
        String[] w = controlMessage.getWindowSize().split("\\.");
        long wSize = Long.parseLong(w[0]); // windowSize parsed in long from incoming frontend message
        String[] s = controlMessage.getSlideSize().split("\\.");
        long sSize = Long.parseLong(s[0]); // slideSize parsed in long from incoming frontend message
        String wUnit = w[1].toUpperCase(); // eg: "MINUTES" or "SECONDS" from incoming frontend message
        String sUnit = s[1].toUpperCase(); // eg: "MINUTES" or "SECONDS" from incoming frontend message
        kafkaStream
                .partitionCustom(new HashPartition(), "id")
                .windowAll(SlidingEventTimeWindows.of(Time.of(wSize, TimeUnit.valueOf(wUnit)), Time.of(sSize, TimeUnit.valueOf(sUnit))))
                .process(
                        new ProcessAllWindowFunction<Edge<Vertex, Vertex>, Edge<Vertex, Vertex>, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<Edge<Vertex, Vertex>> iterable,
                                                Collector<Edge<Vertex, Vertex>> collector) throws Exception {
                                for (Edge<Vertex, Vertex> value : iterable) {
                                    collector.collect(value);
                                }
                            }
                        })
                .keyBy(new KeySelector<Edge<Vertex, Vertex>, String>() {
                    // collecting Data
                    @Override
                    public String getKey(Edge<Vertex, Vertex> value) throws Exception {
                        return value.getLabel();
                    }
                })
                // connect with broadcast Stream
                .connect(broadcastControlStream)
                //keyedBraoadcastProcessFunction
                .process(new MyBroadcastProcessFunction<Edge<Vertex, Vertex>>(controlMessage) {
                    protected boolean isInclude(Edge<Vertex, Vertex> value, ControlMessage control) {
                        // do vertex label and edge label filter job
                        boolean vertexLabelIsNull = Objects.isNull(control.getVertexLabel()) || "".equals(control.getVertexLabel());
                        boolean edgeLabelIsNull = Objects.isNull(control.getEdgeLabel()) || "".equals(control.getEdgeLabel());

                        // if vertex and edge label both null, it means do not filter any data
                        if (vertexLabelIsNull && edgeLabelIsNull) {
                            return true;
                        }

                        String sourceVertexLabel = value.getSource().getLabel();
                        String targetVertexLabel = value.getTarget().getLabel();
                        String relationLabel = value.getLabel();

                        return this.doFilter(
                                control,
                                vertexLabelIsNull,
                                edgeLabelIsNull,
                                sourceVertexLabel,
                                targetVertexLabel,
                                relationLabel
                        );
                    }
                })
                .addSink(new MySinkFunction<Edge<Vertex, Vertex>>(controlMessage.getSlideSize()) {
                    @Override
                    public void invoke(Edge<Vertex, Vertex> value, Context context) {
                        graphContainer.addEdge(value);
                        Long timestamp = value.getTimestamp();
                        threshold = 50;
                        send(timestamp);
                    }
                })
                .setParallelism(1);
    }

    /**
     * @param kafkaStream            Consumes the kafkaStream and emits the output based on the windows/slider/vertex/edge to the web socket connected clients
     * @param tableEnv               table environment for grouping
     * @param broadcastControlStream broadcast stream from frontend ui by kafka control topic stream
     * @param controlMessage         current control message
     */
    private static void processWithGrouping(DataStream<Edge<Vertex, Vertex>> kafkaStream,
                                            StreamTableEnvironment tableEnv,
                                            BroadcastStream<ControlMessage> broadcastControlStream,
                                            ControlMessage controlMessage
    ) {
        String windowSize = controlMessage.getWindowSize();
        if (windowSize.contains("milliseconds")) {
            windowSize = windowSize.substring(0, windowSize.length() - 6);
        }
        String slideSize = controlMessage.getSlideSize();
        if (slideSize.contains("milliseconds")) {
            slideSize = slideSize.substring(0, slideSize.length() - 6);
        }
        // Edge<Vertex, Vertex>-->TupleEdge
        DataStream<TupleEdge> tupleEdgeDataStream = kafkaStream
                .map(new MapFunction<Edge<Vertex, Vertex>, TupleEdge>() {
                    @Override
                    public TupleEdge map(Edge<Vertex, Vertex> value) throws Exception {
                        return new TupleEdge(value);
                    }
                });
        // Create a table with 7 fields
        Table table = tableEnv.fromDataStream(tupleEdgeDataStream, "f0, f1, f2, f3, f4, f5, f6.rowtime");


        Table edgeTable = table.window(Slide.over(windowSize).every(slideSize).on("f6")
                //timestamp as statwindow
                .as("statWindow"))
                //grouping, f3 as sourceLabel, f5 as targetLabel, f1 as edgeLabel , f0.count as edgeCount
                .groupBy("statWindow, f1, f3, f5")
                .select("f3 as sourceLabel, f5 as targetLabel, f1 as edgeLabel , f0.count as edgeCount");

//        if (controlMessage.getEdgeLabel() != null && !"".equals(controlMessage.getEdgeLabel())) {
//            edgeTable = edgeTable.filter(String.format("edgeLabel===\"%s\"", controlMessage.getEdgeLabel()));
//        }

        Table sourceTable = table.select("f0,f2 as f1,f3 as f2,f6 as f3");

        Table targetTable = table.select("f0,f4 as f1,f5 as f2,f6 as f3");

        // sourceTable and targetTable combined
        Table vertexTable = sourceTable.unionAll(targetTable);
        // convert the Table into an append DataStream of Row by specifying the class
        DataStream<Row> result = tableEnv.toAppendStream(vertexTable, Types.ROW(Types.STRING(), Types.STRING(), Types.STRING(), Types.SQL_TIMESTAMP()));
        // create a new table with 4 fields
        Table groupedVertexTable = tableEnv.fromDataStream(result, "f0, f1, f2, f3.rowtime")
                .window(Slide.over(windowSize).
                        every(slideSize).
                        on("f3").
                        as("statWindow"))
                //group by timestamp
                .groupBy("statWindow, f2")
                .select("f2 as vertexLabel , f0.count as vertexCount");

//        if (controlMessage.getVertexLabel() != null && !"".equals(controlMessage.getVertexLabel())) {
//            groupedVertexTable = groupedVertexTable.filter(String.format("vertexLabel===\"%s\"", controlMessage.getVertexLabel()));
//        }
        // edgeTable and vertexTable combined
        tableEnv.toAppendStream(edgeTable, Row.class)
                .union(tableEnv.toAppendStream(groupedVertexTable, Row.class))
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        if (value.getArity() > 2) { //grouped edge
                            return value.getField(2).toString();
                        } else {// grouped vertex
                            return value.getField(0).toString();
                        }
                    }
                })
                .connect(broadcastControlStream)
                //KeyedBroadcastProcessFunction
                .process(new MyBroadcastProcessFunction<Row>(controlMessage) {
                    @Override
                    protected boolean isInclude(Row value, ControlMessage control) {
                        // do vertex label and edge label filter job
                        // vertex add by edge
                        if (value.getArity() <= 2) {
                            return false;
                        }

                        boolean vertexLabelIsNull = Objects.isNull(control.getVertexLabel()) || "".equals(control.getVertexLabel());
                        boolean edgeLabelIsNull = Objects.isNull(control.getEdgeLabel()) || "".equals(control.getEdgeLabel());

                        // if vertex and edge label both null, it means do not filter any data
                        if (vertexLabelIsNull && edgeLabelIsNull) {
                            return true;
                        }

                        //grouped edge
                        String sourceVertexLabel = value.getField(0).toString();
                        String targetVertexLabel = value.getField(1).toString();
                        String relationLabel = value.getField(2).toString();

                        return this.doFilter(
                                control,
                                vertexLabelIsNull,
                                edgeLabelIsNull,
                                sourceVertexLabel,
                                targetVertexLabel,
                                relationLabel
                        );
                    }
                })
                .addSink(new MySinkFunction<Row>(controlMessage.getSlideSize()) {
                    @Override
                    public void invoke(Row value, Context context) throws Exception {
                        if (value.getArity() > 2) { //grouped edge
                            graphContainer.addEdge(value.getField(0).toString(), value.getField(1).toString(), value.getField(2), Integer.parseInt(value.getField(3).toString()));
                        } else {// grouped vertex
                            graphContainer.addVertex(value.getField(0), value.getField(0), Integer.parseInt(value.getField(1).toString()));
                        }
                        long timestamp = new Timestamp(System.currentTimeMillis()).getTime();
                        threshold = 10;
                        graphContainer.mergeLabel();
                        send(timestamp);
                    }
                }).setParallelism(1);
    }

    protected static class ControlMessageTriggeredException extends Exception {
        private String controlMessage;

        ControlMessageTriggeredException(String controlMessage) {
            this.controlMessage = controlMessage;
        }

        public String getMessage() {
            return controlMessage;
        }

    }

    protected static abstract class MyBroadcastProcessFunction<T> extends KeyedBroadcastProcessFunction<String, T, ControlMessage, T> {

        protected ControlMessage defaultControlMessage;

        public MyBroadcastProcessFunction(ControlMessage defaultControlMessage) {
            this.defaultControlMessage = defaultControlMessage;
        }

        public void processElement(T value, ReadOnlyContext ctx, Collector<T> out) throws Exception {
            // get keyed broadcast state
            ReadOnlyBroadcastState<String, ControlMessage> controlMessageBroadcastState = ctx.getBroadcastState(controlMessageDescriptor);
            ControlMessage oldControlMessage = controlMessageBroadcastState.get("control");

            // if no broadcast state then assigned by default
            if (oldControlMessage == null)
                oldControlMessage = defaultControlMessage;

            // do filter with control message
            if (!isInclude(value, oldControlMessage)) {
                System.out.println("Value has been filtered: " + value.toString());
                return;
            }
            out.collect(value);
        }

        @Override
        public void processBroadcastElement(ControlMessage value, Context ctx, Collector<T> out) throws Exception {
            BroadcastState<String, ControlMessage> controlMessageBroadcastState = ctx.getBroadcastState(controlMessageDescriptor);
            ControlMessage oldControlMessage = controlMessageBroadcastState.get("control");
            if (oldControlMessage == null) {
                oldControlMessage = defaultControlMessage;
            }

            // update the state value using new state value from broadcast stream
            controlMessageBroadcastState.put("control", value);
            // only the window size、slide size changed or grouping changed
            // we need restart job
            if (!value.isWithGrouping().equals(oldControlMessage.isWithGrouping())) {
                throw new ControlMessageTriggeredException(value.toString());
            }
            if (!value.getSlideSize().equals(oldControlMessage.getSlideSize())
                    || !value.getWindowSize().equals(oldControlMessage.getWindowSize())) {
                throw new ControlMessageTriggeredException(value.toString());
            }

        }

        protected abstract boolean isInclude(T value, ControlMessage control);

        protected boolean doFilter(ControlMessage control, boolean vertexLabelIsNull, boolean edgeLabelIsNull, String sourceVertexLabel, String targetVertexLabel, String relationLabel) {
            boolean hasSameVertexLabel = true;
            boolean hasSameEdgeLabel = true;

            if (!vertexLabelIsNull) {
                hasSameVertexLabel = control.getVertexLabel().contains(sourceVertexLabel)
                        || control.getVertexLabel().contains(targetVertexLabel);
            }

            if (!edgeLabelIsNull) {
                hasSameEdgeLabel = control.getEdgeLabel().contains(relationLabel);
            }

            return hasSameEdgeLabel && hasSameVertexLabel;
        }
    }

    /**
     * when steam has been keyed, there is no accumulator to hold the data,
     * the sink operator will sink every single item to frontend
     * that is terrible thing for frontend,
     * so the sink operator need set a threshold to control the frequency to send data
     *
     * @param <T>
     */
    protected static abstract class MySinkFunction<T> implements SinkFunction<T> {
        protected int threshold = 20;
        protected GraphContainer graphContainer = new GraphContainer();
        //记下来上一次更新的时间
        protected Long lastSunkAt = new Date().getTime();
        protected Long slideSize;

        public MySinkFunction(String slideSize) {
            this.slideSize = getSlideSize(slideSize);
        }

        @Override
        public abstract void invoke(T value, Context context) throws Exception;
//算下一批数据带的时间戳和这个时间戳的大小，如果数据的时间戳小于前端的缓存时间的话，则属于前一个滑动块，则做增量更新，
//如果过来的数据大于的话，则属于下一个，则把原来的清空，做从新的渲染。
        protected void send(long timestamp) {
            if (lastSunkAt + slideSize <= timestamp) {
                Server.sendToAll(graphContainer.toString());
                graphContainer.clear();
                lastSunkAt = timestamp;
                return;
            }

            if (graphContainer.size() >= threshold) {
                Server.sendToAll(graphContainer.toString());
                graphContainer.clear();
            }
        }

        private Long getSlideSize(String slideSize) {
            String[] s = slideSize.split("\\.");
            long sSize = Long.parseLong(s[0]);
            String sUnit = s[1].toUpperCase();
            return Time.of(sSize, TimeUnit.valueOf(sUnit)).toMilliseconds();
        }
    }
}
