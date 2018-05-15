import functions.EmbeddingsReducer;
import functions.VertexFilterFunction;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.util.GradoopFlinkConfig;


public class Main extends AbstractRunner implements ProgramDescription {

  public static void main(String[] args) throws Exception {

    String input = "";
    String output = "";

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    DataSource source = new JSONDataSource(input, conf);

    LogicalGraph graph = source.getLogicalGraph();

    graph.vertexInducedSubgraph(new VertexFilterFunction());

    GraphCollection collection = graph.match(
                "MATCH   (p1:Person)-[:IS_CREATOR]->(po:Post)<-[:LIKES]-(p2:Person)\n" +
                        "(p1)-[:IS_LOCATED_IN]->(c:City)<-[:IS_LOCATED_IN]-(p2)\n" +
                        "(po)<-[:CONTAINER_OF]-(f:Forum)-[:HAS_TAG]->(t:Tag)\n" +
                "WHERE p1 != p2\n" +
                "CONSTRUCT (c)-[new:HAS_INTEREST]->(t)");

    LogicalGraph embeddingsGraph = collection.reduce(new EmbeddingsReducer());

    LogicalGraph groupedGraph  = new Grouping.GroupingBuilder()
      .addVertexGroupingKey("Tag")
      .addVertexGroupingKey("City")
      .addEdgeGroupingKey("HAS_INTEREST")
      .addEdgeAggregator(new CountAggregator())
      .build()
      .execute(embeddingsGraph);

    groupedGraph.transform(
      // GraphHeads
      TransformationFunction.keep(),
      // Vertices
      (current, transformed) -> {
      String[] tokens = current.getLabel().split("_");
      transformed.setLabel(tokens[0]);
      transformed.setProperty("name", tokens[1]);
      transformed.setLabel(current.getLabel());
      return transformed;},
      // Edges
      TransformationFunction.keep());

    DataSink sink = new JSONDataSink(output, conf);
    sink.write(groupedGraph);

    env.execute();
  }

  public String getDescription() {
    return Main.class.getName();
  }
}
