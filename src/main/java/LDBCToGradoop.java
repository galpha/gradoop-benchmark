import functions.*;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.s1ck.ldbc.LDBCToFlink;
import org.s1ck.ldbc.tuples.LDBCEdge;
import org.s1ck.ldbc.tuples.LDBCVertex;

public class LDBCToGradoop extends AbstractRunner implements ProgramDescription {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        LDBCToFlink source = new LDBCToFlink(args[0], env);
        DataSet<LDBCVertex> ldbcVertices = source.getVertices();
        DataSet<LDBCEdge> ldbcEdges = source.getEdges();

        DataSet<ImportVertex<Long>> importVertices  = ldbcVertices
                .map(new LDBCVertrexToImportVertex());

        DataSet<ImportEdge<Long>> importEdge = ldbcEdges
                .map(new LDBCEdgeToImportEdge());

        LogicalGraph graph = new GraphDataSource<>(importVertices, importEdge, config).getLogicalGraph();

        DataSink sink = new JSONDataSink(args[1], config);

        sink.write(graph);

        env.execute();
    }

    public String getDescription() {
        return null;
    }
}
