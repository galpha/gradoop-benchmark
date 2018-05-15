package functions;

import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.ReducibleBinaryGraphToGraphOperator;



public class EmbeddingsReducer implements ReducibleBinaryGraphToGraphOperator {
  public LogicalGraph execute(GraphCollection collection) {
    return collection.getConfig().getLogicalGraphFactory().fromDataSets(
      collection.getVertices(), collection.getEdges());
  }

  public String getName() {
    return EmbeddingsReducer.class.getName();
  }
}
