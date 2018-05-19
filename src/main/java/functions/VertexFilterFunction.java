package functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;



public class VertexFilterFunction implements FilterFunction<Vertex> {
  public boolean filter(Vertex vertex) {

    return vertex.getLabel().equals("person") ||
            vertex.getLabel().equals("tag") ||
            vertex.getLabel().equals("city") ||
            vertex.getLabel().equals("post") ||
            vertex.getLabel().equals("country");
  }
}
