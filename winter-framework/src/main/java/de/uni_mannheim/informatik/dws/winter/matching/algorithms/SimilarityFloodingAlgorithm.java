package de.uni_mannheim.informatik.dws.winter.matching.algorithms;

import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.CoeffEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ColumnEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.DataTypeEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.IPGNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.LabeledEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.NameEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.PairwiseConnectivityNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.SFIdentifier;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.SFLiteral;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.SFNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.SimilarityFloodingSchema;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.TypeEdge;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import de.uni_mannheim.informatik.dws.winter.webtables.MatchableTableColumn;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.slf4j.Logger;

/**
 * Implementation of a Similarity Flooding Algorithm for structured schema matching
 *
 * (Similarity Flooding: A Versatile Graph Matching Algorithm and its Application to Schema Matching)
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class SimilarityFloodingAlgorithm implements MatchingAlgorithm<MatchableTableColumn, MatchableTableColumn> {

    private static final Logger logger = WinterLogManager.getLogger();

    private final SimilarityFloodingSchema schemaA;
    private final SimilarityFloodingSchema schemaB;
    private SimpleDirectedGraph<SFNode, LabeledEdge> schemaGraphA;
    private SimpleDirectedGraph<SFNode, LabeledEdge> schemaGraphB;
    private SimpleDirectedGraph<IPGNode, CoeffEdge> ipg;
    private SimpleDirectedGraph<PairwiseConnectivityNode, LabeledEdge> pcg;
    private ProcessableCollection<Correspondence<MatchableTableColumn, MatchableTableColumn>> result;
    private HashMap<String, MatchableTableColumn> schemaAMap = new HashMap<>();
    private HashMap<String, MatchableTableColumn> schemaBMap = new HashMap<>();

    // Default values for SF
    private double epsilon = 0.000000000000002;
    private int maxSteps = 1000;
    private double defaultSim = 0.0001;

    public SimilarityFloodingAlgorithm(SimilarityFloodingSchema schemaA, SimilarityFloodingSchema schemaB) {
        this.schemaA = schemaA;
        this.schemaB = schemaB;
    }

    public SimilarityFloodingAlgorithm(SimilarityFloodingSchema schemaA, SimilarityFloodingSchema schemaB, double epsilon, int maxSteps) {
        this.schemaA = schemaA;
        this.schemaB = schemaB;
        this.epsilon = epsilon;
        this.maxSteps = maxSteps;
    }

    public void run() {
        SimpleDirectedGraph<SFNode, LabeledEdge> schemaGraphA = createGraphForSchema(schemaA, schemaAMap);
        SimpleDirectedGraph<SFNode, LabeledEdge> schemaGraphB = createGraphForSchema(schemaB, schemaBMap);

        pcg = generatePairwiseConnectivityGraph(schemaGraphA, schemaGraphB);
        ipg = generateInducedPropagationGraph(schemaGraphA, schemaGraphB, pcg);

        similarityFlooding(ipg, maxSteps);

        List<Pair<Pair<SFLiteral, SFLiteral>, Double>> engagements = filterStableMarriage(ipg);

        result = new ProcessableCollection<>();
        logger.trace("Final result after filter application:");
        for (Pair<Pair<SFLiteral, SFLiteral>, Double> entry : engagements) {
            Pair<SFLiteral, SFLiteral> pair = entry.getFirst();
            if (pair.getFirst() == null) {
                logger.trace("no_match_found");
            } else {
                logger.trace(pair.getFirst().toString());
            }
            logger.trace("<->");
            if (pair.getSecond() == null) {
                logger.trace("no_match_found");
            } else {
                logger.trace(pair.getSecond().toString());
            }
            logger.trace("\n");

            // CHECK
            if (pair.getFirst() == null || pair.getSecond() == null || !schemaAMap.containsKey(pair.getFirst().getValue()) || !schemaBMap.containsKey(pair.getSecond().getValue())) {
                continue;
            }

            result.add(new Correspondence<>(schemaAMap.get(pair.getFirst().getValue()), schemaBMap.get(pair.getSecond().getValue()), 0.0));
        }
    }

    private List<Pair<Pair<SFLiteral, SFLiteral>, Double>> filterStableMarriage(SimpleDirectedGraph<IPGNode, CoeffEdge> ipg) {
        List<Pair<Pair<SFLiteral, SFLiteral>, Double>> engagements = new ArrayList<>();

        HashMap<SFLiteral, Pair<SFLiteral, List<Pair<Double, SFLiteral>>>> schema1Engagements = new HashMap<>();
        HashMap<SFLiteral, Pair<Double, SFLiteral>> schema2Engagements = new HashMap<>();
        initEngagementsSchemas(ipg, schema1Engagements, schema2Engagements);

        boolean change = true;
        while (change) {
            change = false;

            for (Entry<SFLiteral, Pair<SFLiteral, List<Pair<Double, SFLiteral>>>> entry : schema1Engagements.entrySet()) {
                if (entry.getValue().getFirst() == null && entry.getValue().getSecond().size() > 0) {
                    List<Pair<Double, SFLiteral>> possibleMarriages = entry.getValue().getSecond();
                    Pair<Double, SFLiteral> proposal = possibleMarriages.remove(0);

                    if (schema2Engagements.get(proposal.getSecond()) == null) {
                        schema2Engagements.put(proposal.getSecond(), new Pair<>(proposal.getFirst(), entry.getKey()));
                        entry.setValue(new Pair<>(proposal.getSecond(), entry.getValue().getSecond()));
                    } else {
                        if (schema2Engagements.get(proposal.getSecond()).getFirst() < proposal.getFirst()) {
                            Pair<SFLiteral, List<Pair<Double, SFLiteral>>> oldPair = schema1Engagements.get(schema2Engagements.get(proposal.getSecond()).getSecond());
                            schema1Engagements.put(schema2Engagements.get(proposal.getSecond()).getSecond(), new Pair<>(null, oldPair.getSecond()));
                            schema2Engagements.put(proposal.getSecond(), new Pair<>(proposal.getFirst(), entry.getKey()));
                            entry.setValue(new Pair<>(proposal.getSecond(), entry.getValue().getSecond()));
                        }
                    }
                    change = true;
                }
            }
        }

        for (Entry<SFLiteral, Pair<SFLiteral, List<Pair<Double, SFLiteral>>>> entry : schema1Engagements.entrySet()) {
            engagements.add(new Pair<>(new Pair<>(entry.getKey(), entry.getValue().getFirst()), 0.0));
        }

        for (Entry<SFLiteral, Pair<Double, SFLiteral>> entry : schema2Engagements.entrySet()) {
            if (entry.getValue() == null) {
                engagements.add(new Pair<>(new Pair<>(null, entry.getKey()), 0.0));
            }
        }

        return engagements;
    }

    private void initEngagementsSchemas(SimpleDirectedGraph<IPGNode, CoeffEdge> ipg, HashMap<SFLiteral, Pair<SFLiteral, List<Pair<Double, SFLiteral>>>> schema1Engagements,
        HashMap<SFLiteral, Pair<Double, SFLiteral>> schema2Engagements) {
        HashMap<SFLiteral, HashMap<SFLiteral, Double>> schema = clearSfCompressed(ipg);

        for (Entry<SFLiteral, HashMap<SFLiteral, Double>> nodeAEntry : schema.entrySet()) {
            SFLiteral nodeA = nodeAEntry.getKey();

            for (Entry<SFLiteral, Double> nodeBEntry : nodeAEntry.getValue().entrySet()) {
                SFLiteral nodeB = nodeBEntry.getKey();

                if (!schema1Engagements.containsKey(nodeA)) {
                    schema1Engagements.put(nodeA, new Pair<>(null, new ArrayList<>()));
                }
                schema1Engagements.get(nodeA).getSecond().add(new Pair<>(schema.get(nodeA).get(nodeB), nodeB));
                if (!schema2Engagements.containsKey(nodeB)) {
                    schema2Engagements.put(nodeB, null);
                }
            }
        }

        for (Pair<SFLiteral, List<Pair<Double, SFLiteral>>> l : schema1Engagements.values()) {
            l.getSecond().sort(Comparator.<Pair<Double, SFLiteral>>comparingDouble(Pair::getFirst).reversed());
        }
    }

    private HashMap<SFLiteral, HashMap<SFLiteral, Double>> clearSfCompressed(SimpleDirectedGraph<IPGNode, CoeffEdge> ipg) {

        // TODO
        double minSim = 0.00000000000001;

        HashMap<SFLiteral, HashMap<SFLiteral, Double>> nodeSimMap = new HashMap<>();
        for (IPGNode node : ipg.vertexSet()) {
            SFNode nodeA = node.getPairwiseConnectivityNode().getA();
            SFNode nodeB = node.getPairwiseConnectivityNode().getB();
            if (node.getCurrSim() > minSim && nodeA instanceof SFLiteral && nodeB instanceof SFLiteral) {
                if (!nodeSimMap.containsKey((SFLiteral) nodeA)) {
                    nodeSimMap.put((SFLiteral) nodeA, new HashMap<>());
                }
                nodeSimMap.get((SFLiteral) nodeA).put((SFLiteral) nodeB, node.getCurrSim());
            }
        }
        return nodeSimMap;
    }

    void similarityFlooding(SimpleDirectedGraph<IPGNode, CoeffEdge> ipg, int maxSteps) {
        for (int i = 0; i < maxSteps; i++) {
            boolean cont = similarityFloodingStep(ipg);

            if (!cont) {
                logger.trace("Terminated: vector has length less than epsilon");
                break;
            }
        }
    }

    boolean similarityFloodingStep(SimpleDirectedGraph<IPGNode, CoeffEdge> ipg) {
        double maxSim = 0;
        double delta = 0;

        for (IPGNode node : ipg.vertexSet()) {
            double newSim = fixpointIncremental(node, ipg);
            maxSim = Math.max(maxSim, newSim);
            node.setNextSim(newSim);
        }

        for (IPGNode node : ipg.vertexSet()) {
            double newCurrSim = node.getNextSim() / maxSim;
            delta += Math.pow((node.getCurrSim() - newCurrSim), 2);
            node.setCurrSim(newCurrSim);
        }

        return Math.sqrt(delta) >= epsilon;
    }

    private double fixpointIncremental(IPGNode node, SimpleDirectedGraph<IPGNode, CoeffEdge> ipg) {
        double increment = 0;

        for (CoeffEdge coeffEdge : ipg.incomingEdgesOf(node)) {
            increment += ipg.getEdgeSource(coeffEdge).getCurrSim() * coeffEdge.getCoeff();
        }

        return node.getCurrSim() + increment;
    }

    SimpleDirectedGraph<PairwiseConnectivityNode, LabeledEdge> generatePairwiseConnectivityGraph(SimpleDirectedGraph<SFNode, LabeledEdge> schemaGraphA,
        SimpleDirectedGraph<SFNode, LabeledEdge> schemaGraphB) {
        SimpleDirectedGraph<PairwiseConnectivityNode, LabeledEdge> pcg = new SimpleDirectedGraph<>(LabeledEdge.class);
        HashMap<SFNode, HashMap<SFNode, PairwiseConnectivityNode>> nodeMap = new HashMap<>();
        for (LabeledEdge edgeFromA : schemaGraphA.edgeSet()) {

            Class<? extends LabeledEdge> edgeClass = null;
            if (edgeFromA instanceof ColumnEdge) {
                edgeClass = ColumnEdge.class;
            } else if (edgeFromA instanceof DataTypeEdge) {
                edgeClass = DataTypeEdge.class;
            } else if (edgeFromA instanceof TypeEdge) {
                edgeClass = TypeEdge.class;
            } else if (edgeFromA instanceof NameEdge) {
                edgeClass = NameEdge.class;
            }

            for (LabeledEdge edgeFromB : schemaGraphB.edgeSet()) {
                if (edgeClass.isInstance(edgeFromB)) {

                    PairwiseConnectivityNode sourceNode;
                    if (!nodeMap.containsKey(schemaGraphA.getEdgeSource(edgeFromA))) {
                        nodeMap.put(schemaGraphA.getEdgeSource(edgeFromA), new HashMap<>());
                    }
                    if (!nodeMap.get(schemaGraphA.getEdgeSource(edgeFromA)).containsKey(schemaGraphB.getEdgeSource(edgeFromB))) {
                        sourceNode = new PairwiseConnectivityNode(schemaGraphA.getEdgeSource(edgeFromA), schemaGraphB.getEdgeSource(edgeFromB));
                        nodeMap.get(schemaGraphA.getEdgeSource(edgeFromA)).put(schemaGraphB.getEdgeSource(edgeFromB), sourceNode);
                    } else {
                        sourceNode = nodeMap.get(schemaGraphA.getEdgeSource(edgeFromA)).get(schemaGraphB.getEdgeSource(edgeFromB));
                    }

                    PairwiseConnectivityNode targetNode;
                    if (!nodeMap.containsKey(schemaGraphA.getEdgeTarget(edgeFromA))) {
                        nodeMap.put(schemaGraphA.getEdgeTarget(edgeFromA), new HashMap<>());
                    }
                    if (!nodeMap.get(schemaGraphA.getEdgeTarget(edgeFromA)).containsKey(schemaGraphB.getEdgeTarget(edgeFromB))) {
                        targetNode = new PairwiseConnectivityNode(schemaGraphA.getEdgeTarget(edgeFromA), schemaGraphB.getEdgeTarget(edgeFromB));
                        nodeMap.get(schemaGraphA.getEdgeTarget(edgeFromA)).put(schemaGraphB.getEdgeTarget(edgeFromB), targetNode);
                    } else {
                        targetNode = nodeMap.get(schemaGraphA.getEdgeTarget(edgeFromA)).get(schemaGraphB.getEdgeTarget(edgeFromB));
                    }

                    pcg.addVertex(sourceNode);
                    pcg.addVertex(targetNode);

                    pcg.addEdge(sourceNode, targetNode, createNewClassFromType(edgeClass));
                }
            }
        }
        return pcg;
    }

    SimpleDirectedGraph<IPGNode, CoeffEdge> generateInducedPropagationGraph(SimpleDirectedGraph<SFNode, LabeledEdge> schemaGraphA, SimpleDirectedGraph<SFNode, LabeledEdge> schemaGraphB,
        SimpleDirectedGraph<PairwiseConnectivityNode, LabeledEdge> pcg) {
        HashMap<SFNode, HashMap<SFNode, Double>> simMap = generateInitialMap(schemaGraphA, schemaGraphB);

        SimpleDirectedGraph<IPGNode, CoeffEdge> ipg = new SimpleDirectedGraph<>(CoeffEdge.class);
        HashMap<PairwiseConnectivityNode, IPGNode> nodeMap = new HashMap<>();

        for (LabeledEdge pcgEdge : pcg.edgeSet()) {

            IPGNode nodeA = createIPGNodeFromPCGNode(pcg.getEdgeSource(pcgEdge), nodeMap, simMap);
            ipg.addVertex(nodeA);
            IPGNode nodeB = createIPGNodeFromPCGNode(pcg.getEdgeTarget(pcgEdge), nodeMap, simMap);
            ipg.addVertex(nodeB);

            //TODO
            String label = "";
            if (pcgEdge instanceof ColumnEdge) {
                label = "column";
            } else if (pcgEdge instanceof DataTypeEdge) {
                label = "dataType";
            } else if (pcgEdge instanceof TypeEdge) {
                label = "type";
            } else if (pcgEdge instanceof NameEdge) {
                label = "name";
            }

            CoeffEdge coeffEdgeA = new CoeffEdge(inverseProduct(pcg.getEdgeSource(pcgEdge), pcg).get(label));
            ipg.addEdge(nodeA, nodeB, coeffEdgeA);
            CoeffEdge coeffEdgeB = new CoeffEdge(inverseProduct(pcg.getEdgeTarget(pcgEdge), pcg).get(label));
            ipg.addEdge(nodeB, nodeA, coeffEdgeB);
        }
        return ipg;
    }

    SimpleDirectedGraph<SFNode, LabeledEdge> createGraphForSchema(SimilarityFloodingSchema schema, HashMap<String, MatchableTableColumn> schemaMap) {

        SimpleDirectedGraph<SFNode, LabeledEdge> graph = new SimpleDirectedGraph<>(LabeledEdge.class);

        String currOid = "&1";

        // prepare data type nodes but add only if datatype exists
        SFIdentifier columnOidStringDataTypeNode = null;
        SFLiteral stringSFLiteral = new SFLiteral("string");

        SFIdentifier columnOidNumericDataTypeNode = null;
        SFLiteral nunericSFLiteral = new SFLiteral("numeric");

        SFIdentifier columnOidDateDataTypeNode = null;
        SFLiteral dateSFLiteral = new SFLiteral("date");

        // prepare Column type node
        SFIdentifier columnTypeNode = new SFIdentifier("Column");
        graph.addVertex(columnTypeNode);
        // prepare ColumnType type node
        SFIdentifier columnTypeTypeNode = new SFIdentifier("ColumnType");
        graph.addVertex(columnTypeTypeNode);
        // prepare Table type node
        SFIdentifier tableTypeNode = new SFIdentifier("Table");
        graph.addVertex(tableTypeNode);

        // Node for table metadata
        SFIdentifier tableOidNode = new SFIdentifier(currOid);
        graph.addVertex(tableOidNode);

        SFLiteral tableNameNode = new SFLiteral(schema.getTableName());
        graph.addVertex(tableNameNode);

        // type edge
        graph.addEdge(tableOidNode, tableTypeNode, new TypeEdge());

        // name edge
        graph.addEdge(tableNameNode, tableOidNode, new NameEdge());

        for (MatchableTableColumn column : schema.getColumns()) {
            SFLiteral columnNameNode = new SFLiteral(column.getHeader());
            schemaMap.put(column.getHeader(), column);
            graph.addVertex(columnNameNode);

            currOid = nextOid(currOid);
            SFIdentifier columnOidNode = new SFIdentifier(currOid);
            graph.addVertex(columnOidNode);

            // column edge
            graph.addEdge(tableOidNode, columnOidNode, new ColumnEdge());

            // name edge
            graph.addEdge(columnOidNode, columnNameNode, new NameEdge());

            // type edge
            graph.addEdge(columnOidNode, columnTypeNode, new TypeEdge());

            // dataType edge
            if (column.getType().equals(DataType.date)) {
                if (columnOidDateDataTypeNode == null) {
                    currOid = nextOid(currOid);
                    columnOidDateDataTypeNode = new SFIdentifier(currOid);
                    graph.addVertex(columnOidDateDataTypeNode);
                    graph.addVertex(dateSFLiteral);

                    // name edge
                    graph.addEdge(columnOidDateDataTypeNode, dateSFLiteral, new NameEdge());

                    // type edge
                    graph.addEdge(columnOidDateDataTypeNode, columnTypeTypeNode, new TypeEdge());
                }
                graph.addEdge(columnOidNode, columnOidDateDataTypeNode, new DataTypeEdge());
            } else if (column.getType().equals(DataType.string)) {
                if (columnOidStringDataTypeNode == null) {
                    currOid = nextOid(currOid);
                    columnOidStringDataTypeNode = new SFIdentifier(currOid);
                    graph.addVertex(columnOidStringDataTypeNode);
                    graph.addVertex(stringSFLiteral);

                    // name edge
                    graph.addEdge(columnOidStringDataTypeNode, stringSFLiteral, new NameEdge());

                    // type edge
                    graph.addEdge(columnOidStringDataTypeNode, columnTypeTypeNode, new TypeEdge());
                }
                graph.addEdge(columnOidNode, columnOidStringDataTypeNode, new DataTypeEdge());
            } else {
                if (columnOidNumericDataTypeNode == null) {
                    currOid = nextOid(currOid);
                    columnOidNumericDataTypeNode = new SFIdentifier(currOid);
                    graph.addVertex(columnOidNumericDataTypeNode);
                    graph.addVertex(nunericSFLiteral);

                    // name edge
                    graph.addEdge(columnOidNumericDataTypeNode, nunericSFLiteral, new NameEdge());

                    // type edge
                    graph.addEdge(columnOidNumericDataTypeNode, columnTypeTypeNode, new TypeEdge());
                }
                graph.addEdge(columnOidNode, columnOidNumericDataTypeNode, new DataTypeEdge());
            }
        }

        return graph;
    }

    private IPGNode createIPGNodeFromPCGNode(PairwiseConnectivityNode pcg, HashMap<PairwiseConnectivityNode, IPGNode> nodeMap, HashMap<SFNode, HashMap<SFNode, Double>> simMap) {
        IPGNode ipgNode;

        if (!nodeMap.containsKey(pcg)) {
            double initSim;
            if (!simMap.containsKey(pcg.getA()) || simMap.containsKey(pcg.getA()) && !simMap.get(pcg.getA()).containsKey(pcg.getB())) {
                initSim = this.defaultSim;
            } else {
                initSim = simMap.get(pcg.getA()).get(pcg.getB());
            }
            ipgNode = new IPGNode(pcg, initSim, initSim, 0);
            nodeMap.put(pcg, ipgNode);
        } else {
            ipgNode = nodeMap.get(pcg);
        }
        return ipgNode;
    }

    private HashMap<String, Double> inverseProduct(PairwiseConnectivityNode node, SimpleDirectedGraph<PairwiseConnectivityNode, LabeledEdge> pcd) {
        HashMap<String, List<PairwiseConnectivityNode>> nodeByLabels = partitionNeighboursByLabels(node, pcd);

        HashMap<String, Double> result = new HashMap<>();
        for (Entry<String, List<PairwiseConnectivityNode>> label : nodeByLabels.entrySet()) {
            result.put(label.getKey(), 1 / (double) label.getValue().size());
        }

        return result;
    }


    private HashMap<String, List<PairwiseConnectivityNode>> partitionNeighboursByLabels(PairwiseConnectivityNode node, SimpleDirectedGraph<PairwiseConnectivityNode, LabeledEdge> pcd) {
        HashMap<String, List<PairwiseConnectivityNode>> neighboursByLabels = new HashMap<>();

        for (LabeledEdge outgoingEdge : pcd.outgoingEdgesOf(node)) {

            //TODO
            String label = "";
            if (outgoingEdge instanceof ColumnEdge) {
                label = "column";
            } else if (outgoingEdge instanceof DataTypeEdge) {
                label = "dataType";
            } else if (outgoingEdge instanceof TypeEdge) {
                label = "type";
            } else if (outgoingEdge instanceof NameEdge) {
                label = "name";
            }

            if (!neighboursByLabels.containsKey(label)) {
                neighboursByLabels.put(label, new ArrayList<>());
            }

            neighboursByLabels.get(label).add(pcd.getEdgeTarget(outgoingEdge));
        }

        for (LabeledEdge incomingEdge : pcd.incomingEdgesOf(node)) {
            String label = "";
            if (incomingEdge instanceof ColumnEdge) {
                label = "column";
            } else if (incomingEdge instanceof DataTypeEdge) {
                label = "dataType";
            } else if (incomingEdge instanceof TypeEdge) {
                label = "type";
            } else if (incomingEdge instanceof NameEdge) {
                label = "name";
            }

            if (!neighboursByLabels.containsKey(label)) {
                neighboursByLabels.put(label, new ArrayList<>());
            }

            neighboursByLabels.get(label).add(pcd.getEdgeSource(incomingEdge));
        }

        return neighboursByLabels;
    }

    private HashMap<SFNode, HashMap<SFNode, Double>> generateInitialMap(SimpleDirectedGraph<SFNode, LabeledEdge> schemaGraphA, SimpleDirectedGraph<SFNode, LabeledEdge> schemaGraphB) {
        HashMap<SFNode, HashMap<SFNode, Double>> simMap = new HashMap<>();
        LevenshteinSimilarity levenshtein = new LevenshteinSimilarity();

        for (SFNode nodeFromA : schemaGraphA.vertexSet()) {
            if (!nodeFromA.getValue().contains("&")) {
                for (SFNode nodeFromB : schemaGraphB.vertexSet()) {
                    if (!nodeFromB.getValue().contains("&")) {

                        double sim;
                        if (nodeFromA.getValue().equals("string") && nodeFromB.getValue().equals("numeric") || nodeFromA.getValue().equals("numeric") && nodeFromB.getValue().equals("string")) {
                            sim = 0.0;
                        } else {
                            sim = levenshtein.calculate(nodeFromA.getValue(), nodeFromB.getValue());
                        }

                        if (!simMap.containsKey(nodeFromA)) {
                            simMap.put(nodeFromA, new HashMap<>());
                        }

                        simMap.get(nodeFromA).put(nodeFromB, sim);
                    }
                }
            }
        }
        return simMap;
    }

    private LabeledEdge createNewClassFromType(Class<? extends LabeledEdge> edgeClass) {
        try {
            return edgeClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String nextOid(String currOid) {
        int oidInteger = Integer.parseInt(currOid.split("&")[1]);
        return "&" + (oidInteger + 1);
    }

    public SimpleDirectedGraph<IPGNode, CoeffEdge> getIpg() {
        return ipg;
    }


    public void setEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }

    public void setMaxSteps(int maxSteps) {
        this.maxSteps = maxSteps;
    }

    public void setDefaultSim(double defaultSim) {
        this.defaultSim = defaultSim;
    }

    @Override
    public Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> getResult() {
        return result;
    }
}
