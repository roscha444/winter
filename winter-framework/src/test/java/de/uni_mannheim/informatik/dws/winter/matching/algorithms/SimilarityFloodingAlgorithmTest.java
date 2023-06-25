package de.uni_mannheim.informatik.dws.winter.matching.algorithms;

import static junit.framework.TestCase.assertEquals;

import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.SimilarityFloodingSchema;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.CoeffEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.IPGNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.LabeledEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.LabeledEdgeType;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.PairwiseConnectivityNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNodeType;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.webtables.MatchableTableColumn;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.junit.Test;

/**
 * Test for {@link SimilarityFloodingAlgorithm}
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class SimilarityFloodingAlgorithmTest {

    @Test
    public void testGraphShouldBeCreatedFromSchema() {
        // prepare
        SimilarityFloodingAlgorithm similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm(null, null);

        List<MatchableTableColumn> tablesForSchemaA = new ArrayList<>();
        MatchableTableColumn pno = new MatchableTableColumn(0, 0, "Pno", DataType.numeric);
        tablesForSchemaA.add(pno);
        MatchableTableColumn pname = new MatchableTableColumn(0, 1, "Pname", DataType.string);
        tablesForSchemaA.add(pname);
        MatchableTableColumn dept = new MatchableTableColumn(0, 2, "Dept", DataType.string);
        tablesForSchemaA.add(dept);

        SimilarityFloodingSchema schemaA = new SimilarityFloodingSchema("Personnel", tablesForSchemaA);

        //execute
        SimpleDirectedGraph<SFNode, LabeledEdge> schemaGraphA = similarityFloodingAlgorithm.createGraphForSchema(schemaA, new HashMap<>());

        //validate
        assertEquals(18, schemaGraphA.edgeSet().size());
        assertEquals(15, schemaGraphA.vertexSet().size());
    }

    @Test
    public void testPairWiseConnectivityGraphShouldBeCreatedFromGraph() {
        // prepare
        SimilarityFloodingAlgorithm similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm(null, null);

        SimpleDirectedGraph<SFNode, LabeledEdge> modelA = new SimpleDirectedGraph<>(LabeledEdge.class);
        SFNode a = new SFNode("a", SFNodeType.IDENTIFIER);
        modelA.addVertex(a);
        SFNode a1 = new SFNode("a1", SFNodeType.IDENTIFIER);
        modelA.addVertex(a1);
        SFNode a2 = new SFNode("a2", SFNodeType.IDENTIFIER);
        modelA.addVertex(a2);

        modelA.addEdge(a, a1, new LabeledEdge(LabeledEdgeType.NAME));
        modelA.addEdge(a, a2, new LabeledEdge(LabeledEdgeType.NAME));
        modelA.addEdge(a1, a2, new LabeledEdge(LabeledEdgeType.TYPE));

        SimpleDirectedGraph<SFNode, LabeledEdge> modelB = new SimpleDirectedGraph<>(LabeledEdge.class);

        SFNode b = new SFNode("b", SFNodeType.IDENTIFIER);
        modelB.addVertex(b);

        SFNode b1 = new SFNode("b1", SFNodeType.IDENTIFIER);
        modelB.addVertex(b1);

        SFNode b2 = new SFNode("b2", SFNodeType.IDENTIFIER);
        modelB.addVertex(b2);

        modelB.addEdge(b, b1, new LabeledEdge(LabeledEdgeType.NAME));
        modelB.addEdge(b, b2, new LabeledEdge(LabeledEdgeType.TYPE));
        modelB.addEdge(b2, b1, new LabeledEdge(LabeledEdgeType.TYPE));

        //execute
        SimpleDirectedGraph<PairwiseConnectivityNode, LabeledEdge> pairwiseConnectivityGraph = similarityFloodingAlgorithm.generatePairwiseConnectivityGraph(modelA, modelB);

        //validate
        assertEquals(4, pairwiseConnectivityGraph.edgeSet().size());
        assertEquals(6, pairwiseConnectivityGraph.vertexSet().size());
    }

    @Test
    public void testInducedPropagationGraphShouldBeCreatedFromPCG() {
        // prepare
        SimilarityFloodingAlgorithm similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm(null, null);

        SimpleDirectedGraph<SFNode, LabeledEdge> modelA = new SimpleDirectedGraph<>(LabeledEdge.class);
        SFNode a = new SFNode("a", SFNodeType.IDENTIFIER);
        modelA.addVertex(a);
        SFNode a1 = new SFNode("a1", SFNodeType.IDENTIFIER);
        modelA.addVertex(a1);
        SFNode a2 = new SFNode("a2", SFNodeType.IDENTIFIER);
        modelA.addVertex(a2);

        modelA.addEdge(a, a1, new LabeledEdge(LabeledEdgeType.NAME));
        modelA.addEdge(a, a2, new LabeledEdge(LabeledEdgeType.NAME));
        modelA.addEdge(a1, a2, new LabeledEdge(LabeledEdgeType.TYPE));

        SimpleDirectedGraph<SFNode, LabeledEdge> modelB = new SimpleDirectedGraph<>(LabeledEdge.class);

        SFNode b = new SFNode("b", SFNodeType.IDENTIFIER);
        modelB.addVertex(b);

        SFNode b1 = new SFNode("b1", SFNodeType.IDENTIFIER);
        modelB.addVertex(b1);

        SFNode b2 = new SFNode("b2", SFNodeType.IDENTIFIER);
        modelB.addVertex(b2);

        modelB.addEdge(b, b1, new LabeledEdge(LabeledEdgeType.NAME));
        modelB.addEdge(b, b2, new LabeledEdge(LabeledEdgeType.TYPE));
        modelB.addEdge(b2, b1, new LabeledEdge(LabeledEdgeType.TYPE));

        SimpleDirectedGraph<PairwiseConnectivityNode, LabeledEdge> pairwiseConnectivityGraph = similarityFloodingAlgorithm.generatePairwiseConnectivityGraph(modelA, modelB);
        //execute
        SimpleDirectedGraph<IPGNode, CoeffEdge> inducedPropagationGraph = similarityFloodingAlgorithm.generateInducedPropagationGraph(modelA, modelB, pairwiseConnectivityGraph);

        //validate
        assertEquals(8, inducedPropagationGraph.edgeSet().size());
        assertEquals(6, inducedPropagationGraph.vertexSet().size());
    }

    @Test
    public void testShouldCalculateFixpointValuesFromIPG_1() {
        // prepare
        SimilarityFloodingAlgorithm similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm(null, null);

        SimpleDirectedGraph<SFNode, LabeledEdge> modelA = new SimpleDirectedGraph<>(LabeledEdge.class);
        SFNode a = new SFNode("a", SFNodeType.IDENTIFIER);
        modelA.addVertex(a);
        SFNode a1 = new SFNode("a1", SFNodeType.IDENTIFIER);
        modelA.addVertex(a1);
        SFNode a2 = new SFNode("a2", SFNodeType.IDENTIFIER);
        modelA.addVertex(a2);

        modelA.addEdge(a, a1, new LabeledEdge(LabeledEdgeType.NAME));
        modelA.addEdge(a, a2, new LabeledEdge(LabeledEdgeType.NAME));
        modelA.addEdge(a1, a2, new LabeledEdge(LabeledEdgeType.TYPE));

        SimpleDirectedGraph<SFNode, LabeledEdge> modelB = new SimpleDirectedGraph<>(LabeledEdge.class);

        SFNode b = new SFNode("b", SFNodeType.IDENTIFIER);
        modelB.addVertex(b);

        SFNode b1 = new SFNode("b1", SFNodeType.IDENTIFIER);
        modelB.addVertex(b1);

        SFNode b2 = new SFNode("b2", SFNodeType.IDENTIFIER);
        modelB.addVertex(b2);

        modelB.addEdge(b, b1, new LabeledEdge(LabeledEdgeType.NAME));
        modelB.addEdge(b, b2, new LabeledEdge(LabeledEdgeType.TYPE));
        modelB.addEdge(b2, b1, new LabeledEdge(LabeledEdgeType.TYPE));

        SimpleDirectedGraph<PairwiseConnectivityNode, LabeledEdge> pairwiseConnectivityGraph = similarityFloodingAlgorithm.generatePairwiseConnectivityGraph(modelA, modelB);
        SimpleDirectedGraph<IPGNode, CoeffEdge> inducedPropagationGraph = similarityFloodingAlgorithm.generateInducedPropagationGraph(modelA, modelB, pairwiseConnectivityGraph);
        //execute
        similarityFloodingAlgorithm.similarityFlooding(inducedPropagationGraph, 1000);

        //validate
        Object[] vertex = inducedPropagationGraph.vertexSet().toArray();
        assertEquals(1.00, ((IPGNode) vertex[0]).getCurrSim(), 0.1);
        assertEquals(0.91, ((IPGNode) vertex[2]).getCurrSim(), 0.1);
        assertEquals(0.69, ((IPGNode) vertex[5]).getCurrSim(), 0.1);
        assertEquals(0.39, ((IPGNode) vertex[1]).getCurrSim(), 0.1);
    }

    @Test
    public void testShouldCalculateFixpointValuesFromIPG_2() {
        // prepare
        List<MatchableTableColumn> columnsSchemaOne = new ArrayList<>();
        MatchableTableColumn pno = new MatchableTableColumn(0, 0, "Pno", DataType.numeric);
        columnsSchemaOne.add(pno);
        MatchableTableColumn pname = new MatchableTableColumn(0, 1, "Pname", DataType.string);
        columnsSchemaOne.add(pname);
        MatchableTableColumn dept = new MatchableTableColumn(0, 2, "Dept", DataType.string);
        columnsSchemaOne.add(dept);
        MatchableTableColumn born = new MatchableTableColumn(0, 3, "Born", DataType.date);
        columnsSchemaOne.add(born);

        SimilarityFloodingSchema schema1 = new SimilarityFloodingSchema("Personnel", columnsSchemaOne);

        List<MatchableTableColumn> columnsSchemaTwo = new ArrayList<>();
        MatchableTableColumn empNo = new MatchableTableColumn(1, 0, "EmpNo", DataType.numeric);
        columnsSchemaTwo.add(empNo);
        MatchableTableColumn empName = new MatchableTableColumn(1, 1, "EmpName", DataType.string);
        columnsSchemaTwo.add(empName);
        MatchableTableColumn deptNo = new MatchableTableColumn(1, 2, "DepNo", DataType.numeric);
        columnsSchemaTwo.add(deptNo);
        MatchableTableColumn birthdate = new MatchableTableColumn(1, 3, "Birthdate", DataType.date);
        columnsSchemaTwo.add(birthdate);

        SimilarityFloodingSchema schema2 = new SimilarityFloodingSchema("Employee", columnsSchemaTwo);

        SimilarityFloodingAlgorithm similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm(schema1, schema2);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        HashMap<String, HashMap<String, Double>> nodeSimMap = getResultMap(similarityFloodingAlgorithm);

        assertEquals(0.43, nodeSimMap.get("Personnel").get("Employee"), 0.1);
        assertEquals(0.035, nodeSimMap.get("Pno").get("EmpName"), 0.1);
        assertEquals(0.035, nodeSimMap.get("Born").get("Birthdate"), 0.1);
        assertEquals(0.03, nodeSimMap.get("numeric").get("string"), 0.1);
        assertEquals(0.03, nodeSimMap.get("date").get("date"), 0.1);
    }

    @Test
    public void testShouldCalculateFixpointValuesFromIPG_2_REMOVE_OID() {
        // prepare
        List<MatchableTableColumn> columnsSchemaOne = new ArrayList<>();
        MatchableTableColumn pno = new MatchableTableColumn(0, 0, "Pno", DataType.numeric);
        columnsSchemaOne.add(pno);
        MatchableTableColumn pname = new MatchableTableColumn(0, 1, "Pname", DataType.string);
        columnsSchemaOne.add(pname);
        MatchableTableColumn dept = new MatchableTableColumn(0, 2, "Dept", DataType.string);
        columnsSchemaOne.add(dept);
        MatchableTableColumn born = new MatchableTableColumn(0, 3, "Born", DataType.date);
        columnsSchemaOne.add(born);

        SimilarityFloodingSchema schema1 = new SimilarityFloodingSchema("Personnel", columnsSchemaOne);

        List<MatchableTableColumn> columnsSchemaTwo = new ArrayList<>();
        MatchableTableColumn empNo = new MatchableTableColumn(1, 0, "EmpNo", DataType.numeric);
        columnsSchemaTwo.add(empNo);
        MatchableTableColumn empName = new MatchableTableColumn(1, 1, "EmpName", DataType.string);
        columnsSchemaTwo.add(empName);
        MatchableTableColumn deptNo = new MatchableTableColumn(1, 2, "DepNo", DataType.numeric);
        columnsSchemaTwo.add(deptNo);
        MatchableTableColumn birthdate = new MatchableTableColumn(1, 3, "Birthdate", DataType.date);
        columnsSchemaTwo.add(birthdate);

        SimilarityFloodingSchema schema2 = new SimilarityFloodingSchema("Employee", columnsSchemaTwo);

        SimilarityFloodingAlgorithm similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm(schema1, schema2);
        similarityFloodingAlgorithm.setRemoveOid(true);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        HashMap<String, HashMap<String, Double>> nodeSimMap = getResultMap(similarityFloodingAlgorithm);

        assertEquals(1.0, nodeSimMap.get("Personnel").get("Employee"), 0.1);
        assertEquals(0.08, nodeSimMap.get("Pno").get("EmpName"), 0.1);
        assertEquals(0.08, nodeSimMap.get("Born").get("Birthdate"), 0.1);
        assertEquals(0.07, nodeSimMap.get("numeric").get("string"), 0.1);
        assertEquals(0.07, nodeSimMap.get("date").get("date"), 0.1);
    }

    @Test
    public void testShouldRunSimilarityFloodingAlgorithm_1() {
        // prepare
        List<MatchableTableColumn> columnsSchemaOne = new ArrayList<>();
        MatchableTableColumn pno = new MatchableTableColumn(0, 0, "Pno", DataType.numeric);
        columnsSchemaOne.add(pno);
        MatchableTableColumn pname = new MatchableTableColumn(0, 1, "Pname", DataType.string);
        columnsSchemaOne.add(pname);
        MatchableTableColumn dept = new MatchableTableColumn(0, 2, "Dept", DataType.string);
        columnsSchemaOne.add(dept);
        MatchableTableColumn born = new MatchableTableColumn(0, 3, "Born", DataType.date);
        columnsSchemaOne.add(born);

        SimilarityFloodingSchema schema1 = new SimilarityFloodingSchema("Personnel", columnsSchemaOne);

        List<MatchableTableColumn> columnsSchemaTwo = new ArrayList<>();
        MatchableTableColumn empNo = new MatchableTableColumn(1, 0, "EmpNo", DataType.numeric);
        columnsSchemaTwo.add(empNo);
        MatchableTableColumn empName = new MatchableTableColumn(1, 1, "EmpName", DataType.string);
        columnsSchemaTwo.add(empName);
        MatchableTableColumn deptNo = new MatchableTableColumn(1, 2, "DepNo", DataType.numeric);
        columnsSchemaTwo.add(deptNo);
        MatchableTableColumn salary = new MatchableTableColumn(1, 3, "Salary", DataType.numeric);
        columnsSchemaTwo.add(salary);
        MatchableTableColumn birthdate = new MatchableTableColumn(1, 4, "Birthdate", DataType.date);
        columnsSchemaTwo.add(birthdate);

        SimilarityFloodingSchema schema2 = new SimilarityFloodingSchema("Employee", columnsSchemaTwo);

        SimilarityFloodingAlgorithm similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm(schema1, schema2);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> result = similarityFloodingAlgorithm.getResult();

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<MatchableTableColumn, MatchableTableColumn> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getHeader(), correspondence.getSecondRecord().getHeader());
        }

        assertEquals(birthdate.getHeader(), resultList.get(born.getHeader()));
        assertEquals(empName.getHeader(), resultList.get(pno.getHeader()));
        assertEquals(deptNo.getHeader(), resultList.get(dept.getHeader()));
        assertEquals(empNo.getHeader(), resultList.get(pname.getHeader()));
    }

    @Test
    public void testShouldRunSimilarityFloodingAlgorithm_1_REMOVE_OID() {
        // prepare
        List<MatchableTableColumn> columnsSchemaOne = new ArrayList<>();
        MatchableTableColumn pno = new MatchableTableColumn(0, 0, "Pno", DataType.numeric);
        columnsSchemaOne.add(pno);
        MatchableTableColumn pname = new MatchableTableColumn(0, 1, "Pname", DataType.string);
        columnsSchemaOne.add(pname);
        MatchableTableColumn dept = new MatchableTableColumn(0, 2, "Dept", DataType.string);
        columnsSchemaOne.add(dept);
        MatchableTableColumn born = new MatchableTableColumn(0, 3, "Born", DataType.date);
        columnsSchemaOne.add(born);

        SimilarityFloodingSchema schema1 = new SimilarityFloodingSchema("Personnel", columnsSchemaOne);

        List<MatchableTableColumn> columnsSchemaTwo = new ArrayList<>();
        MatchableTableColumn empNo = new MatchableTableColumn(1, 0, "EmpNo", DataType.numeric);
        columnsSchemaTwo.add(empNo);
        MatchableTableColumn empName = new MatchableTableColumn(1, 1, "EmpName", DataType.string);
        columnsSchemaTwo.add(empName);
        MatchableTableColumn deptNo = new MatchableTableColumn(1, 2, "DepNo", DataType.numeric);
        columnsSchemaTwo.add(deptNo);
        MatchableTableColumn salary = new MatchableTableColumn(1, 3, "Salary", DataType.numeric);
        columnsSchemaTwo.add(salary);
        MatchableTableColumn birthdate = new MatchableTableColumn(1, 4, "Birthdate", DataType.date);
        columnsSchemaTwo.add(birthdate);

        SimilarityFloodingSchema schema2 = new SimilarityFloodingSchema("Employee", columnsSchemaTwo);

        SimilarityFloodingAlgorithm similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm(schema1, schema2);
        similarityFloodingAlgorithm.setRemoveOid(true);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> result = similarityFloodingAlgorithm.getResult();

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<MatchableTableColumn, MatchableTableColumn> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getHeader(), correspondence.getSecondRecord().getHeader());
        }

        assertEquals(birthdate.getHeader(), resultList.get(born.getHeader()));
        assertEquals(empName.getHeader(), resultList.get(pno.getHeader()));
        assertEquals(deptNo.getHeader(), resultList.get(pname.getHeader()));
        assertEquals(salary.getHeader(), resultList.get(dept.getHeader()));
    }

    private HashMap<String, HashMap<String, Double>> getResultMap(SimilarityFloodingAlgorithm similarityFloodingAlgorithm) {
        SimpleDirectedGraph<IPGNode, CoeffEdge> ipg = similarityFloodingAlgorithm.getIpg();
        List<IPGNode> ipgList = ipg.vertexSet().stream().collect(Collectors.toList());
        List<IPGNode> ipgListWithoutOid = ipgList.stream()
            .filter(x -> !x.getPairwiseConnectivityNode().getA().getValue().contains("&") && !x.getPairwiseConnectivityNode().getB().getValue().contains("&")).collect(Collectors.toList());

        HashMap<String, HashMap<String, Double>> nodeSimMap = new HashMap<>();
        for (IPGNode node : ipgListWithoutOid) {
            String a = node.getPairwiseConnectivityNode().getA().getValue();
            String b = node.getPairwiseConnectivityNode().getB().getValue();
            Double sim = node.getCurrSim();
            if (!nodeSimMap.containsKey(a)) {
                nodeSimMap.put(a, new HashMap<>());
            }
            nodeSimMap.get(a).put(b, sim);
        }
        return nodeSimMap;
    }

}
