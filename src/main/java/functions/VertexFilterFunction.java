package functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.Vertex;



public class VertexFilterFunction implements FilterFunction<Vertex> {
  public boolean filter(Vertex vertex) throws Exception {

    boolean pass =  vertex.getLabel().equals("Person") ||
            vertex.getLabel().equals("Forum") ||
            vertex.getLabel().equals("Tag") ||
            vertex.getLabel().equals("City") ||
            vertex.getLabel().equals("Post");

    return pass;
  }
}
