package org.bsc.langgraph4j.poc;


import org.bsc.async.AsyncGenerator;
import org.bsc.langgraph4j.*;
import org.bsc.langgraph4j.action.AsyncNodeAction;
import org.bsc.langgraph4j.checkpoint.MemorySaver;
import org.bsc.langgraph4j.serializer.std.ObjectStreamStateSerializer;
import org.bsc.langgraph4j.state.AgentState;
import org.bsc.langgraph4j.state.Channel;
import org.bsc.langgraph4j.state.Channels;
import org.bsc.langgraph4j.streaming.StreamingOutput;
import org.bsc.langgraph4j.utils.EdgeMappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class UpdateStateWithNode {

    public static void main(String[] args) throws Exception {

        CompiledGraph compiledGraph = compiledGraph();

        RunnableConfig runnableConfig = RunnableConfig.builder()
                .threadId("1")
                .build();
        
        
        var stream = compiledGraph.stream(Map.of(), runnableConfig);
        extracted(stream);
        System.out.println("First Time execute Done , Execute order NodeA - NodeB - END");

        // Resume from NODE_B
        System.out.println("Resume execute Done , Resume from NodeB , and Set NEXT_NODE , want to route to FEEDBACK_NODE");
        RunnableConfig existedConfig = RunnableConfig.builder()
                .threadId("1")
                .build();
        RunnableConfig updatedConfig = compiledGraph.updateState(existedConfig, Map.of("NEXT_NODE","FEEDBACK_NODE"), "NODE_B");
        var resumeStream = compiledGraph.stream(GraphInput.resume(), updatedConfig);
        extracted(resumeStream);
        System.out.println("Resume execute Done , Resume from Node B  Execute order NodeA - NodeB - END");


    }

    private static void extracted(AsyncGenerator.Cancellable stream) {
        Iterator iterator = stream.stream().iterator();
        while (iterator.hasNext()){
            Object next = iterator.next();
            if( next instanceof StreamingOutput<?> streamingOutput){
                System.out.println("Chunk : " + streamingOutput.chunk());
            }else if (next instanceof NodeOutput<?> nodeOutput){
                AgentState state = nodeOutput.state();
                String nodeName = nodeOutput.node();
                System.out.println("NodeName : " + nodeName + " Executed ");
                System.out.println("Result State Display, NodeName : " + nodeName + " State : " + state);
            }
        }
    }


    public static StateGraph buildStateGraph() throws GraphStateException {

        Map<String, Channel<?>> channels = new HashMap<>();
        channels.put("VALUE", Channels.appender(ArrayList::new));

        StateGraph<AgentState> stateGraph = new StateGraph<>(channels, new ObjectStreamStateSerializer<>(AgentState::new));

        stateGraph.addNode("NODE_A", (AsyncNodeAction<AgentState>) state -> {
            System.out.println("NODE_A Executing");
            return completedFuture(Map.of("VALUE", "1"));
        });

        stateGraph.addNode("NODE_B", (AsyncNodeAction<AgentState>) state -> {
            System.out.println("NODE_B Executing");
            return completedFuture(Map.of("VALUE", "2"));
        });

        stateGraph.addNode("FEEDBACK_NODE", (AsyncNodeAction<AgentState>) state -> {
            System.out.println("FEEDBACK_NODE Executing");
            return completedFuture(Map.of("VALUE", "3"));
        });

        stateGraph.addEdge(StateGraph.START, "NODE_A");
        stateGraph.addEdge("NODE_A", "NODE_B");

        stateGraph.addConditionalEdges(
                "NODE_B",
                state -> completedFuture(getStringValue(state,"NEXT_NODE",StateGraph.END)),
                new EdgeMappings.Builder()
                        .to("FEEDBACK_NODE")
                        .toEND()
                        .build()
        );

        stateGraph.addEdge("FEEDBACK_NODE",StateGraph.END);
        return stateGraph;
    }

    public static CompiledGraph compiledGraph() throws GraphStateException {
        var saver = new MemorySaver();
        CompileConfig compileConfig = CompileConfig.builder()
                .interruptBefore("FEEDBACK_NODE")
                .checkpointSaver(saver)
                .releaseThread(false)
                .build();

        StateGraph stateGraph = buildStateGraph();
        CompiledGraph compile = stateGraph.compile(compileConfig);
        GraphRepresentation mermaidGraph = compile.getGraph(GraphRepresentation.Type.MERMAID);
        System.out.println("=== Workflow Graph (MERMAID) === \n " + mermaidGraph);
        System.out.println("\n ");
        System.out.println("\n ");
        return compile;

    }

    public static String getStringValue(AgentState state, String key, String defaultValue) {
        return state.value(key).map(String.class::cast).orElse(defaultValue);
    }

    public static String getStringValue(AgentState state, String key) {
        return state.value(key)
                .map(String.class::cast)
                .orElseThrow(() -> new IllegalStateException("State key not found: " + key));
    }
}
