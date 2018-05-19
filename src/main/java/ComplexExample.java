import functions.EdgeFilterFunction;
import functions.VertexFilterFunction;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.util.GradoopFlinkConfig;


public class ComplexExample extends AbstractRunner implements ProgramDescription {

  public static void main(String[] args) throws Exception {

    String input = args[0];
    String output = args[1];

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    DataSource source = new JSONDataSource(input, conf);

    LogicalGraph graph = source.getLogicalGraph();

    graph = graph.vertexInducedSubgraph(new VertexFilterFunction());

    GraphCollection embeddings = graph.cypher(
                "MATCH (p1:person)<-[:hasCreator]-(po:post)<-[:likes]-(p2:person)\n" +
                            "(p1)-[:isLocatedIn]->(c1:city)\n" +
                            "(p2)-[:isLocatedIn]->(c2:city)" +
                            "(po)-[:hasTag]->(t:tag)\n" +
                            "(c1)-[:isPartOf]->(ca:country)<-[:isPartOf]-(c2)\n" +
                      "WHERE p1 != p2",
              "CONSTRUCT (ca)-[new:hasInterest]->(t)");


    LogicalGraph embeddingsGraph = embeddings.reduce(new ReduceCombination());

    LogicalGraph groupedGraph  = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .addVertexGroupingKey("name")
      .useEdgeLabel(true).useVertexLabel(true)
      .addEdgeAggregator(new CountAggregator())
      .build().execute(embeddingsGraph).edgeInducedSubgraph(new EdgeFilterFunction());

    DataSink sink = new DOTDataSink(args[1], false);
    sink.write(groupedGraph);

    env.execute();
  }

  public String getDescription() {
    return ComplexExample.class.getName();
  }
}
