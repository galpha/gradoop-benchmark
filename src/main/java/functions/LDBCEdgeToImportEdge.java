package functions;

import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.s1ck.ldbc.tuples.LDBCEdge;

import java.util.HashMap;

public class LDBCEdgeToImportEdge implements org.apache.flink.api.common.functions.MapFunction<org.s1ck.ldbc.tuples.LDBCEdge, org.gradoop.flink.io.impl.graph.tuples.ImportEdge<Long>> {

    private ImportEdge<Long> reuse;

    public LDBCEdgeToImportEdge() {
        reuse = new ImportEdge<>();
    }

    public ImportEdge<Long> map(LDBCEdge ldbcEdge) {
        reuse.setId(ldbcEdge.getEdgeId());
        reuse.setLabel(ldbcEdge.getLabel());
        reuse.setSourceId(ldbcEdge.getSourceVertexId());
        reuse.setTargetId(ldbcEdge.getTargetVertexId());
        reuse.setProperties(Properties.createFromMap(new HashMap<>()));
        return reuse;
    }
}
