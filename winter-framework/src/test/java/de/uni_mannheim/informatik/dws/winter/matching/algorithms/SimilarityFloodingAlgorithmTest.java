package de.uni_mannheim.informatik.dws.winter.matching.algorithms;

import static junit.framework.TestCase.assertEquals;

import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.Filter;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.FixpointFormula;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.CoeffEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.IPGNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.LabeledEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.LabeledEdgeType;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.PairwiseConnectivityNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNodeType;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.ComparatorLogger;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import de.uni_mannheim.informatik.dws.winter.webtables.SFMatchable;
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

    public static class SFComparatorLevenshtein implements Comparator<SFTestMatchable, SFTestMatchable> {

        private static final long serialVersionUID = 1L;
        private final LevenshteinSimilarity similarity = new LevenshteinSimilarity();
        private ComparatorLogger comparisonLog;


        @Override
        public double compare(SFTestMatchable record1, SFTestMatchable record2, Correspondence<SFTestMatchable, Matchable> schemaCorrespondence) {
            return similarity.calculate(record1.getValue(), record2.getValue());
        }

        @Override
        public ComparatorLogger getComparisonLog() {
            return this.comparisonLog;
        }

        @Override
        public void setComparisonLog(ComparatorLogger comparatorLog) {
            this.comparisonLog = comparatorLog;
        }

    }

    private static class SFTestMatchable extends SFMatchable {

        private final String id;
        private final String value;

        public SFTestMatchable(DataType type, String id, String value) {
            super(type);
            this.id = id;
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String getIdentifier() {
            return id;
        }

        @Override
        public String getProvenance() {
            return null;
        }

        @Override
        public String toString() {
            return "SFTestMatchable{" +
                "id='" + id + '\'' +
                ", value='" + value + '\'' +
                '}';
        }
    }

    @Test
    public void testGraphShouldBeCreatedFromSchema() {
        // prepare
        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>(null, null, new SFComparatorLevenshtein(), FixpointFormula.A);

        List<SFTestMatchable> tablesForSchemaA = new ArrayList<>();
        SFTestMatchable pno = new SFTestMatchable(DataType.numeric, "0", "Pno");
        tablesForSchemaA.add(pno);
        SFTestMatchable pname = new SFTestMatchable(DataType.string, "1", "Pname");
        tablesForSchemaA.add(pname);
        SFTestMatchable dept = new SFTestMatchable(DataType.string, "2", "Dept");
        tablesForSchemaA.add(dept);

        //execute
        SimpleDirectedGraph<SFNode<SFTestMatchable>, LabeledEdge> schemaGraphA = similarityFloodingAlgorithm.createGraphForSchema(tablesForSchemaA, "Personnel");

        //validate
        assertEquals(18, schemaGraphA.edgeSet().size());
        assertEquals(15, schemaGraphA.vertexSet().size());
    }

    @Test
    public void testPairWiseConnectivityGraphShouldBeCreatedFromGraph() {
        // prepare
        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>(null, null, new SFComparatorLevenshtein(), FixpointFormula.A);

        SimpleDirectedGraph<SFNode<SFTestMatchable>, LabeledEdge> modelA = new SimpleDirectedGraph<>(LabeledEdge.class);
        SFNode<SFTestMatchable> a = new SFNode<>("a", SFNodeType.IDENTIFIER);
        modelA.addVertex(a);
        SFNode<SFTestMatchable> a1 = new SFNode<>("a1", SFNodeType.IDENTIFIER);
        modelA.addVertex(a1);
        SFNode<SFTestMatchable> a2 = new SFNode<>("a2", SFNodeType.IDENTIFIER);
        modelA.addVertex(a2);

        modelA.addEdge(a, a1, new LabeledEdge(LabeledEdgeType.NAME));
        modelA.addEdge(a, a2, new LabeledEdge(LabeledEdgeType.NAME));
        modelA.addEdge(a1, a2, new LabeledEdge(LabeledEdgeType.TYPE));

        SimpleDirectedGraph<SFNode<SFTestMatchable>, LabeledEdge> modelB = new SimpleDirectedGraph<>(LabeledEdge.class);

        SFNode<SFTestMatchable> b = new SFNode<>("b", SFNodeType.IDENTIFIER);
        modelB.addVertex(b);

        SFNode<SFTestMatchable> b1 = new SFNode<>("b1", SFNodeType.IDENTIFIER);
        modelB.addVertex(b1);

        SFNode<SFTestMatchable> b2 = new SFNode<>("b2", SFNodeType.IDENTIFIER);
        modelB.addVertex(b2);

        modelB.addEdge(b, b1, new LabeledEdge(LabeledEdgeType.NAME));
        modelB.addEdge(b, b2, new LabeledEdge(LabeledEdgeType.TYPE));
        modelB.addEdge(b2, b1, new LabeledEdge(LabeledEdgeType.TYPE));

        //execute
        SimpleDirectedGraph<PairwiseConnectivityNode<SFTestMatchable>, LabeledEdge> pairwiseConnectivityGraph = similarityFloodingAlgorithm.generatePairwiseConnectivityGraph(modelA, modelB);

        //validate
        assertEquals(4, pairwiseConnectivityGraph.edgeSet().size());
        assertEquals(6, pairwiseConnectivityGraph.vertexSet().size());
    }

    @Test
    public void testInducedPropagationGraphShouldBeCreatedFromPCG() {
        // prepare
        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>(null, null, null, FixpointFormula.A);

        SimpleDirectedGraph<SFNode<SFTestMatchable>, LabeledEdge> modelA = new SimpleDirectedGraph<>(LabeledEdge.class);
        SFNode<SFTestMatchable> a = new SFNode<>("a", SFNodeType.IDENTIFIER);
        modelA.addVertex(a);
        SFNode<SFTestMatchable> a1 = new SFNode<>("a1", SFNodeType.IDENTIFIER);
        modelA.addVertex(a1);
        SFNode<SFTestMatchable> a2 = new SFNode<>("a2", SFNodeType.IDENTIFIER);
        modelA.addVertex(a2);

        modelA.addEdge(a, a1, new LabeledEdge(LabeledEdgeType.NAME));
        modelA.addEdge(a, a2, new LabeledEdge(LabeledEdgeType.NAME));
        modelA.addEdge(a1, a2, new LabeledEdge(LabeledEdgeType.TYPE));

        SimpleDirectedGraph<SFNode<SFTestMatchable>, LabeledEdge> modelB = new SimpleDirectedGraph<>(LabeledEdge.class);

        SFNode<SFTestMatchable> b = new SFNode<>("b", SFNodeType.IDENTIFIER);
        modelB.addVertex(b);

        SFNode<SFTestMatchable> b1 = new SFNode<>("b1", SFNodeType.IDENTIFIER);
        modelB.addVertex(b1);

        SFNode<SFTestMatchable> b2 = new SFNode<>("b2", SFNodeType.IDENTIFIER);
        modelB.addVertex(b2);

        modelB.addEdge(b, b1, new LabeledEdge(LabeledEdgeType.NAME));
        modelB.addEdge(b, b2, new LabeledEdge(LabeledEdgeType.TYPE));
        modelB.addEdge(b2, b1, new LabeledEdge(LabeledEdgeType.TYPE));

        SimpleDirectedGraph<PairwiseConnectivityNode<SFTestMatchable>, LabeledEdge> pairwiseConnectivityGraph = similarityFloodingAlgorithm.generatePairwiseConnectivityGraph(modelA, modelB);
        //execute
        SimpleDirectedGraph<IPGNode<SFTestMatchable>, CoeffEdge> inducedPropagationGraph = similarityFloodingAlgorithm.generateInducedPropagationGraph(modelA, modelB, pairwiseConnectivityGraph);

        //validate
        assertEquals(8, inducedPropagationGraph.edgeSet().size());
        assertEquals(6, inducedPropagationGraph.vertexSet().size());
    }

    @Test
    public void testShouldCalculateFixpointValuesFromIPG_1() {
        // prepare
        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>(null, null, new SFComparatorLevenshtein(), FixpointFormula.A);

        SimpleDirectedGraph<SFNode<SFTestMatchable>, LabeledEdge> modelA = new SimpleDirectedGraph<>(LabeledEdge.class);
        SFNode<SFTestMatchable> a = new SFNode<>("a", SFNodeType.IDENTIFIER);
        modelA.addVertex(a);
        SFNode<SFTestMatchable> a1 = new SFNode<>("a1", SFNodeType.IDENTIFIER);
        modelA.addVertex(a1);
        SFNode<SFTestMatchable> a2 = new SFNode<>("a2", SFNodeType.IDENTIFIER);
        modelA.addVertex(a2);

        modelA.addEdge(a, a1, new LabeledEdge(LabeledEdgeType.NAME));
        modelA.addEdge(a, a2, new LabeledEdge(LabeledEdgeType.NAME));
        modelA.addEdge(a1, a2, new LabeledEdge(LabeledEdgeType.TYPE));

        SimpleDirectedGraph<SFNode<SFTestMatchable>, LabeledEdge> modelB = new SimpleDirectedGraph<>(LabeledEdge.class);

        SFNode<SFTestMatchable> b = new SFNode<>("b", SFNodeType.IDENTIFIER);
        modelB.addVertex(b);

        SFNode<SFTestMatchable> b1 = new SFNode<>("b1", SFNodeType.IDENTIFIER);
        modelB.addVertex(b1);

        SFNode<SFTestMatchable> b2 = new SFNode<>("b2", SFNodeType.IDENTIFIER);
        modelB.addVertex(b2);

        modelB.addEdge(b, b1, new LabeledEdge(LabeledEdgeType.NAME));
        modelB.addEdge(b, b2, new LabeledEdge(LabeledEdgeType.TYPE));
        modelB.addEdge(b2, b1, new LabeledEdge(LabeledEdgeType.TYPE));

        SimpleDirectedGraph<PairwiseConnectivityNode<SFTestMatchable>, LabeledEdge> pairwiseConnectivityGraph = similarityFloodingAlgorithm.generatePairwiseConnectivityGraph(modelA, modelB);
        SimpleDirectedGraph<IPGNode<SFTestMatchable>, CoeffEdge> inducedPropagationGraph = similarityFloodingAlgorithm.generateInducedPropagationGraph(modelA, modelB, pairwiseConnectivityGraph);
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
        List<SFTestMatchable> columnsSchemaOne = new ArrayList<>();
        SFTestMatchable pno = new SFTestMatchable(DataType.numeric, "Pno", "Pno");
        columnsSchemaOne.add(pno);
        SFTestMatchable pname = new SFTestMatchable(DataType.string, "Pname", "Pname");
        columnsSchemaOne.add(pname);
        SFTestMatchable dept = new SFTestMatchable(DataType.string, "Dept", "Dept");
        columnsSchemaOne.add(dept);
        SFTestMatchable born = new SFTestMatchable(DataType.date, "Born", "Born");
        columnsSchemaOne.add(born);

        List<SFTestMatchable> columnsSchemaTwo = new ArrayList<>();
        SFTestMatchable empNo = new SFTestMatchable(DataType.numeric, "EmpNo", "EmpNo");
        columnsSchemaTwo.add(empNo);
        SFTestMatchable empName = new SFTestMatchable(DataType.string, "EmpName", "EmpName");
        columnsSchemaTwo.add(empName);
        SFTestMatchable deptNo = new SFTestMatchable(DataType.numeric, "DepNo", "DepNo");
        columnsSchemaTwo.add(deptNo);
        SFTestMatchable birthdate = new SFTestMatchable(DataType.date, "Birthdate", "Birthdate");
        columnsSchemaTwo.add(birthdate);

        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>("Personnel", columnsSchemaOne, "Employee", columnsSchemaTwo,
            new SFComparatorLevenshtein(), FixpointFormula.Basic);

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
        List<SFTestMatchable> columnsSchemaOne = new ArrayList<>();
        SFTestMatchable pno = new SFTestMatchable(DataType.numeric, "Pno", "Pno");
        columnsSchemaOne.add(pno);
        SFTestMatchable pname = new SFTestMatchable(DataType.string, "Pname", "Pname");
        columnsSchemaOne.add(pname);
        SFTestMatchable dept = new SFTestMatchable(DataType.string, "Dept", "Dept");
        columnsSchemaOne.add(dept);
        SFTestMatchable born = new SFTestMatchable(DataType.date, "Born", "Born");
        columnsSchemaOne.add(born);

        List<SFTestMatchable> columnsSchemaTwo = new ArrayList<>();
        SFTestMatchable empNo = new SFTestMatchable(DataType.numeric, "EmpNo", "EmpNo");
        columnsSchemaTwo.add(empNo);
        SFTestMatchable empName = new SFTestMatchable(DataType.string, "EmpName", "EmpName");
        columnsSchemaTwo.add(empName);
        SFTestMatchable deptNo = new SFTestMatchable(DataType.numeric, "DepNo", "DepNo");
        columnsSchemaTwo.add(deptNo);
        SFTestMatchable birthdate = new SFTestMatchable(DataType.date, "Birthdate", "Birthdate");
        columnsSchemaTwo.add(birthdate);

        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>("Personnel", columnsSchemaOne, "Employee", columnsSchemaTwo,
            new SFComparatorLevenshtein(), FixpointFormula.A);
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
        List<SFTestMatchable> columnsSchemaOne = new ArrayList<>();
        SFTestMatchable pno = new SFTestMatchable(DataType.numeric, "Pno", "Pno");
        columnsSchemaOne.add(pno);
        SFTestMatchable pname = new SFTestMatchable(DataType.string, "Pname", "Pname");
        columnsSchemaOne.add(pname);
        SFTestMatchable dept = new SFTestMatchable(DataType.string, "Dept", "Dept");
        columnsSchemaOne.add(dept);
        SFTestMatchable born = new SFTestMatchable(DataType.date, "Born", "Born");
        columnsSchemaOne.add(born);

        List<SFTestMatchable> columnsSchemaTwo = new ArrayList<>();
        SFTestMatchable empNo = new SFTestMatchable(DataType.numeric, "EmpNo", "EmpNo");
        columnsSchemaTwo.add(empNo);
        SFTestMatchable empName = new SFTestMatchable(DataType.numeric, "EmpName", "EmpName");
        columnsSchemaTwo.add(empName);
        SFTestMatchable deptNo = new SFTestMatchable(DataType.numeric, "DepNo", "DepNo");
        columnsSchemaTwo.add(deptNo);
        SFTestMatchable salary = new SFTestMatchable(DataType.numeric, "Salary", "Salary");
        columnsSchemaTwo.add(salary);
        SFTestMatchable birthdate = new SFTestMatchable(DataType.date, "Birthdate", "Birthdate");
        columnsSchemaTwo.add(birthdate);

        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>("Personnel", columnsSchemaOne, "Employee", columnsSchemaTwo,
            new SFComparatorLevenshtein(), FixpointFormula.A);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        Processable<Correspondence<SFTestMatchable, SFTestMatchable>> result = similarityFloodingAlgorithm.getResult();

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<SFTestMatchable, SFTestMatchable> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getValue(), correspondence.getSecondRecord().getValue());
        }

        assertEquals(birthdate.getValue(), resultList.get(born.getValue()));
        assertEquals(empNo.getValue(), resultList.get(pno.getValue()));
        assertEquals(deptNo.getValue(), resultList.get(dept.getValue()));
        assertEquals(salary.getValue(), resultList.get(pname.getValue()));
    }

    @Test
    public void testShouldRunSimilarityFloodingAlgorithm_1_REMOVE_OID() {
        // prepare
        List<SFTestMatchable> columnsSchemaOne = new ArrayList<>();
        SFTestMatchable pno = new SFTestMatchable(DataType.numeric, "Pno", "Pno");
        columnsSchemaOne.add(pno);
        SFTestMatchable pname = new SFTestMatchable(DataType.string, "Pname", "Pname");
        columnsSchemaOne.add(pname);
        SFTestMatchable dept = new SFTestMatchable(DataType.string, "Dept", "Dept");
        columnsSchemaOne.add(dept);
        SFTestMatchable born = new SFTestMatchable(DataType.date, "Born", "Born");
        columnsSchemaOne.add(born);

        List<SFTestMatchable> columnsSchemaTwo = new ArrayList<>();
        SFTestMatchable empNo = new SFTestMatchable(DataType.numeric, "EmpNo", "EmpNo");
        columnsSchemaTwo.add(empNo);
        SFTestMatchable empName = new SFTestMatchable(DataType.string, "EmpName", "EmpName");
        columnsSchemaTwo.add(empName);
        SFTestMatchable deptNo = new SFTestMatchable(DataType.numeric, "DepNo", "DepNo");
        columnsSchemaTwo.add(deptNo);
        SFTestMatchable salary = new SFTestMatchable(DataType.numeric, "Salary", "Salary");
        columnsSchemaTwo.add(salary);
        SFTestMatchable birthdate = new SFTestMatchable(DataType.date, "Birthdate", "Birthdate");
        columnsSchemaTwo.add(birthdate);

        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>("Personnel", columnsSchemaOne, "Employee", columnsSchemaTwo,
            new SFComparatorLevenshtein(), FixpointFormula.A);
        similarityFloodingAlgorithm.setRemoveOid(true);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        Processable<Correspondence<SFTestMatchable, SFTestMatchable>> result = similarityFloodingAlgorithm.getResult();

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<SFTestMatchable, SFTestMatchable> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getValue(), correspondence.getSecondRecord().getValue());
        }

        assertEquals(birthdate.getValue(), resultList.get(born.getValue()));
        assertEquals(empName.getValue(), resultList.get(pno.getValue()));
        assertEquals(deptNo.getValue(), resultList.get(pname.getValue()));
        assertEquals(salary.getValue(), resultList.get(dept.getValue()));
    }

    @Test
    public void testShouldRunSimilarityFloodingAlgorithm_2_REMOVE_OID() {
        // prepare
        List<SFTestMatchable> columnsSchemaOne = new ArrayList<>();
        SFTestMatchable areaTotal = new SFTestMatchable(DataType.numeric, "areatotal", "areatotal");
        columnsSchemaOne.add(areaTotal);
        SFTestMatchable city = new SFTestMatchable(DataType.string, "city", "city");
        columnsSchemaOne.add(city);
        SFTestMatchable country = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaOne.add(country);

        List<SFTestMatchable> columnsSchemaTwo = new ArrayList<>();
        SFTestMatchable areaTotal2 = new SFTestMatchable(DataType.numeric, "areaTotal", "areaTotal");
        columnsSchemaTwo.add(areaTotal2);
        SFTestMatchable country2 = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaTwo.add(country2);
        SFTestMatchable state = new SFTestMatchable(DataType.string, "state", "state");
        columnsSchemaTwo.add(state);

        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>("Personnel", columnsSchemaOne, "Employee", columnsSchemaTwo,
            new SFComparatorLevenshtein(), FixpointFormula.A);
        similarityFloodingAlgorithm.setRemoveOid(true);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        Processable<Correspondence<SFTestMatchable, SFTestMatchable>> result = similarityFloodingAlgorithm.getResult();

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<SFTestMatchable, SFTestMatchable> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getValue(), correspondence.getSecondRecord().getValue());
        }

        assertEquals(areaTotal2.getValue(), resultList.get(areaTotal.getValue()));
        assertEquals(country2.getValue(), resultList.get(country.getValue()));
        assertEquals(state.getValue(), resultList.get(city.getValue()));
    }

    @Test
    public void testShouldRunSimilarityFloodingAlgorithm_2() {
        // prepare
        List<SFTestMatchable> columnsSchemaOne = new ArrayList<>();
        SFTestMatchable areaTotal = new SFTestMatchable(DataType.numeric, "areatotal", "areatotal");
        columnsSchemaOne.add(areaTotal);
        SFTestMatchable city = new SFTestMatchable(DataType.string, "city", "city");
        columnsSchemaOne.add(city);
        SFTestMatchable country = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaOne.add(country);

        List<SFTestMatchable> columnsSchemaTwo = new ArrayList<>();
        SFTestMatchable areaTotal2 = new SFTestMatchable(DataType.numeric, "areaTotal", "areaTotal");
        columnsSchemaTwo.add(areaTotal2);
        SFTestMatchable country2 = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaTwo.add(country2);
        SFTestMatchable state = new SFTestMatchable(DataType.string, "state", "state");
        columnsSchemaTwo.add(state);

        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>("Personnel", columnsSchemaOne, "Employee", columnsSchemaTwo,
            new SFComparatorLevenshtein(), FixpointFormula.A);
        similarityFloodingAlgorithm.setRemoveOid(false);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        Processable<Correspondence<SFTestMatchable, SFTestMatchable>> result = similarityFloodingAlgorithm.getResult();

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<SFTestMatchable, SFTestMatchable> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getValue(), correspondence.getSecondRecord().getValue());
        }

        assertEquals(areaTotal2.getValue(), resultList.get(areaTotal.getValue()));
        assertEquals(country2.getValue(), resultList.get(country.getValue()));
        assertEquals(state.getValue(), resultList.get(city.getValue()));
    }

    @Test
    public void testShouldRunSimilarityFloodingAlgorithm_3() {
        // prepare
        List<SFTestMatchable> columnsSchemaOne = new ArrayList<>();
        SFTestMatchable areaTotal = new SFTestMatchable(DataType.numeric, "areatotal", "areatotal");
        columnsSchemaOne.add(areaTotal);
        SFTestMatchable city = new SFTestMatchable(DataType.string, "city", "city");
        columnsSchemaOne.add(city);
        SFTestMatchable country = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaOne.add(country);
        SFTestMatchable name = new SFTestMatchable(DataType.string, "name", "name");
        columnsSchemaOne.add(name);

        List<SFTestMatchable> columnsSchemaTwo = new ArrayList<>();
        SFTestMatchable areaTotal2 = new SFTestMatchable(DataType.numeric, "areaTotal", "areaTotal");
        columnsSchemaTwo.add(areaTotal2);
        SFTestMatchable country2 = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaTwo.add(country2);
        SFTestMatchable state = new SFTestMatchable(DataType.string, "state", "state");
        columnsSchemaTwo.add(state);
        SFTestMatchable area = new SFTestMatchable(DataType.string, "area", "area");
        columnsSchemaTwo.add(area);

        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>("Personnel", columnsSchemaOne, "Employee", columnsSchemaTwo,
            new SFComparatorLevenshtein(), FixpointFormula.A);
        similarityFloodingAlgorithm.setRemoveOid(false);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        Processable<Correspondence<SFTestMatchable, SFTestMatchable>> result = similarityFloodingAlgorithm.getResult();

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<SFTestMatchable, SFTestMatchable> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getValue(), correspondence.getSecondRecord().getValue());
        }

        assertEquals(areaTotal2.getValue(), resultList.get(areaTotal.getValue()));
        assertEquals(country2.getValue(), resultList.get(country.getValue()));
        assertEquals(area.getValue(), resultList.get(city.getValue()));
        assertEquals(state.getValue(), resultList.get(name.getValue()));
    }

    @Test
    public void testShouldRunSimilarityFloodingAlgorithm_4_TOP_ONE_K() {
        // prepare
        List<SFTestMatchable> columnsSchemaOne = new ArrayList<>();
        SFTestMatchable areaTotal = new SFTestMatchable(DataType.numeric, "areatotal", "areatotal");
        columnsSchemaOne.add(areaTotal);
        SFTestMatchable city = new SFTestMatchable(DataType.string, "city", "city");
        columnsSchemaOne.add(city);
        SFTestMatchable country = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaOne.add(country);
        SFTestMatchable name = new SFTestMatchable(DataType.string, "name", "name");
        columnsSchemaOne.add(name);

        List<SFTestMatchable> columnsSchemaTwo = new ArrayList<>();
        SFTestMatchable areaTotal2 = new SFTestMatchable(DataType.numeric, "areaTotal", "areaTotal");
        columnsSchemaTwo.add(areaTotal2);
        SFTestMatchable country2 = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaTwo.add(country2);
        SFTestMatchable state = new SFTestMatchable(DataType.string, "state", "state");
        columnsSchemaTwo.add(state);
        SFTestMatchable area = new SFTestMatchable(DataType.string, "area", "area");
        columnsSchemaTwo.add(area);

        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>("Personnel", columnsSchemaOne, "Employee", columnsSchemaTwo,
            new SFComparatorLevenshtein(), FixpointFormula.A);
        similarityFloodingAlgorithm.setRemoveOid(false);
        similarityFloodingAlgorithm.setFilter(Filter.TopOneK);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        Processable<Correspondence<SFTestMatchable, SFTestMatchable>> result = similarityFloodingAlgorithm.getResult();

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<SFTestMatchable, SFTestMatchable> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getValue(), correspondence.getSecondRecord().getValue());
        }

        assertEquals(areaTotal2.getValue(), resultList.get(areaTotal.getValue()));
        assertEquals(country2.getValue(), resultList.get(country.getValue()));
        assertEquals(state.getValue(), resultList.get(city.getValue()));
        assertEquals(area.getValue(), resultList.get(name.getValue()));
    }

    @Test
    public void testShouldRunSimilarityFloodingAlgorithm_5_HUNGARIAN_ALGORITHM() {
        // prepare
        List<SFTestMatchable> columnsSchemaOne = new ArrayList<>();
        SFTestMatchable areaTotal = new SFTestMatchable(DataType.numeric, "areatotal", "areatotal");
        columnsSchemaOne.add(areaTotal);
        SFTestMatchable city = new SFTestMatchable(DataType.string, "city", "city");
        columnsSchemaOne.add(city);
        SFTestMatchable country = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaOne.add(country);
        SFTestMatchable name = new SFTestMatchable(DataType.string, "name", "name");
        columnsSchemaOne.add(name);

        List<SFTestMatchable> columnsSchemaTwo = new ArrayList<>();
        SFTestMatchable areaTotal2 = new SFTestMatchable(DataType.numeric, "areaTotal", "areaTotal");
        columnsSchemaTwo.add(areaTotal2);
        SFTestMatchable country2 = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaTwo.add(country2);
        SFTestMatchable state = new SFTestMatchable(DataType.string, "state", "state");
        columnsSchemaTwo.add(state);
        SFTestMatchable area = new SFTestMatchable(DataType.string, "area", "area");
        columnsSchemaTwo.add(area);

        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>("Personnel", columnsSchemaOne, "Employee", columnsSchemaTwo,
            new SFComparatorLevenshtein(), FixpointFormula.A);
        similarityFloodingAlgorithm.setRemoveOid(true);
        similarityFloodingAlgorithm.setMinSim(0.10);
        similarityFloodingAlgorithm.setFilter(Filter.HungarianAlgorithm);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        Processable<Correspondence<SFTestMatchable, SFTestMatchable>> result = similarityFloodingAlgorithm.getResult();

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<SFTestMatchable, SFTestMatchable> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getValue(), correspondence.getSecondRecord().getValue());
        }

        assertEquals(areaTotal2.getValue(), resultList.get(areaTotal.getValue()));
        assertEquals(country2.getValue(), resultList.get(country.getValue()));
        assertEquals(state.getValue(), resultList.get(city.getValue()));
        assertEquals(area.getValue(), resultList.get(name.getValue()));
    }

    @Test
    public void testShouldRunSimilarityFloodingAlgorithm_5_HUNGARIAN_ALGORITHM_2() {
        // prepare
        List<SFTestMatchable> columnsSchemaOne = new ArrayList<>();
        SFTestMatchable areaTotal = new SFTestMatchable(DataType.numeric, "areatotal", "areatotal");
        columnsSchemaOne.add(areaTotal);
        SFTestMatchable country = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaOne.add(country);

        List<SFTestMatchable> columnsSchemaTwo = new ArrayList<>();
        SFTestMatchable areaTotal2 = new SFTestMatchable(DataType.numeric, "areaTotal", "areaTotal");
        columnsSchemaTwo.add(areaTotal2);
        SFTestMatchable country2 = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaTwo.add(country2);
        SFTestMatchable state = new SFTestMatchable(DataType.string, "state", "state");
        columnsSchemaTwo.add(state);
        SFTestMatchable area = new SFTestMatchable(DataType.string, "area", "area");
        columnsSchemaTwo.add(area);

        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>("Personnel", columnsSchemaOne, "Employee", columnsSchemaTwo,
            new SFComparatorLevenshtein(), FixpointFormula.A);
        similarityFloodingAlgorithm.setRemoveOid(true);
        similarityFloodingAlgorithm.setMinSim(0.10);
        similarityFloodingAlgorithm.setFilter(Filter.HungarianAlgorithm);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        Processable<Correspondence<SFTestMatchable, SFTestMatchable>> result = similarityFloodingAlgorithm.getResult();

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<SFTestMatchable, SFTestMatchable> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getValue(), correspondence.getSecondRecord().getValue());
        }

        assertEquals(areaTotal2.getValue(), resultList.get(areaTotal.getValue()));
        assertEquals(country2.getValue(), resultList.get(country.getValue()));
    }

    @Test
    public void testShouldRunSimilarityFloodingAlgorithm_5_HUNGARIAN_ALGORITHM_3() {
        // prepare
        List<SFTestMatchable> columnsSchemaOne = new ArrayList<>();
        SFTestMatchable areaTotal = new SFTestMatchable(DataType.numeric, "areatotal", "areatotal");
        columnsSchemaOne.add(areaTotal);
        SFTestMatchable country = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaOne.add(country);

        List<SFTestMatchable> columnsSchemaTwo = new ArrayList<>();
        SFTestMatchable country2 = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaTwo.add(country2);
        SFTestMatchable area = new SFTestMatchable(DataType.string, "area", "area");
        columnsSchemaTwo.add(area);

        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>("Personnel", columnsSchemaOne, "Employee", columnsSchemaTwo,
            new SFComparatorLevenshtein(), FixpointFormula.A);
        similarityFloodingAlgorithm.setRemoveOid(false);
        similarityFloodingAlgorithm.setMinSim(0.10);
        similarityFloodingAlgorithm.setFilter(Filter.HungarianAlgorithm);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        Processable<Correspondence<SFTestMatchable, SFTestMatchable>> result = similarityFloodingAlgorithm.getResult();

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<SFTestMatchable, SFTestMatchable> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getValue(), correspondence.getSecondRecord().getValue());
        }

        assertEquals(area.getValue(), resultList.get(areaTotal.getValue()));
        assertEquals(country2.getValue(), resultList.get(country.getValue()));
    }

    @Test
    public void testShouldRunSimilarityFloodingAlgorithm_EXAMPLE_PAPER() {
        // prepare
        List<SFTestMatchable> columnsSchemaOne = new ArrayList<>();
        SFTestMatchable country = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaOne.add(country);
        SFTestMatchable popDens = new SFTestMatchable(DataType.numeric, "pop. dens", "pop. dens");
        columnsSchemaOne.add(popDens);
        SFTestMatchable area = new SFTestMatchable(DataType.numeric, "area", "area");
        columnsSchemaOne.add(area);
        SFTestMatchable population = new SFTestMatchable(DataType.numeric, "population", "population");
        columnsSchemaOne.add(population);

        List<SFTestMatchable> columnsSchemaTwo = new ArrayList<>();
        SFTestMatchable country2 = new SFTestMatchable(DataType.string, "country", "country");
        columnsSchemaTwo.add(country2);
        SFTestMatchable areaTotal = new SFTestMatchable(DataType.numeric, "areaTotal", "areaTotal");
        columnsSchemaTwo.add(areaTotal);
        SFTestMatchable populationDensity = new SFTestMatchable(DataType.numeric, "populationDensity", "populationDensity");
        columnsSchemaTwo.add(populationDensity);
        SFTestMatchable capitallabel = new SFTestMatchable(DataType.string, "capitallabel", "capitallabel");
        columnsSchemaTwo.add(capitallabel);
        SFTestMatchable capital = new SFTestMatchable(DataType.string, "capital", "capital");
        columnsSchemaTwo.add(capital);

        SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm = new SimilarityFloodingAlgorithm<>("CountryWB", columnsSchemaOne, "CountryDB", columnsSchemaTwo,
            new SFComparatorLevenshtein(), FixpointFormula.A);
        similarityFloodingAlgorithm.setRemoveOid(false);
        similarityFloodingAlgorithm.setFilter(Filter.StableMarriage);

        // run
        similarityFloodingAlgorithm.run();

        // validate
        Processable<Correspondence<SFTestMatchable, SFTestMatchable>> result = similarityFloodingAlgorithm.getResult();

        HashMap<String, String> resultList = new HashMap<>();
        for (Correspondence<SFTestMatchable, SFTestMatchable> correspondence : result.get()) {
            resultList.put(correspondence.getFirstRecord().getValue(), correspondence.getSecondRecord().getValue());
        }
    }

    private HashMap<String, HashMap<String, Double>> getResultMap(SimilarityFloodingAlgorithm<SFTestMatchable, SFTestMatchable> similarityFloodingAlgorithm) {
        SimpleDirectedGraph<IPGNode<SFTestMatchable>, CoeffEdge> ipg = similarityFloodingAlgorithm.getIpg();
        List<IPGNode<SFTestMatchable>> ipgList = new ArrayList<>(ipg.vertexSet());
        List<IPGNode<SFTestMatchable>> ipgListWithoutOid = ipgList.stream()
            .filter(x -> !x.getPairwiseConnectivityNode().getA().getGetIdentifier().contains("&") && !x.getPairwiseConnectivityNode().getB().getGetIdentifier().contains("&"))
            .collect(Collectors.toList());

        HashMap<String, HashMap<String, Double>> nodeSimMap = new HashMap<>();
        for (IPGNode<SFTestMatchable> node : ipgListWithoutOid) {
            String a = node.getPairwiseConnectivityNode().getA().getGetIdentifier();
            String b = node.getPairwiseConnectivityNode().getB().getGetIdentifier();
            Double sim = node.getCurrSim();
            if (!nodeSimMap.containsKey(a)) {
                nodeSimMap.put(a, new HashMap<>());
            }
            nodeSimMap.get(a).put(b, sim);
        }
        return nodeSimMap;
    }

}
