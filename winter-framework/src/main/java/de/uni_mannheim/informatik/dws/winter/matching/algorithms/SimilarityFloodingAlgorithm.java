package de.uni_mannheim.informatik.dws.winter.matching.algorithms;

import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.SimilarityFloodingSchema;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.CoeffEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.IPGNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.LabeledEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.LabeledEdgeType;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.PairwiseConnectivityNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNodeType;
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
    private double defaultSim = 0.0;
    private double minSim = 0.00000000000001;

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
        schemaGraphA = createGraphForSchema(schemaA, schemaAMap);
        schemaGraphB = createGraphForSchema(schemaB, schemaBMap);

        pcg = generatePairwiseConnectivityGraph(schemaGraphA, schemaGraphB);
        ipg = generateInducedPropagationGraph(schemaGraphA, schemaGraphB, pcg);

        similarityFlooding(ipg, maxSteps);

        List<Pair<Pair<SFNode, SFNode>, Double>> engagements = filterStableMarriage(ipg);

        result = new ProcessableCollection<>();
        logger.trace("Final result after filter application:");
        for (Pair<Pair<SFNode, SFNode>, Double> entry : engagements) {
            Pair<SFNode, SFNode> pair = entry.getFirst();
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

    private List<Pair<Pair<SFNode, SFNode>, Double>> filterStableMarriage(SimpleDirectedGraph<IPGNode, CoeffEdge> ipg) {
        List<Pair<Pair<SFNode, SFNode>, Double>> engagements = new ArrayList<>();

        HashMap<SFNode, Pair<SFNode, List<Pair<Double, SFNode>>>> schema1Engagements = new HashMap<>();
        HashMap<SFNode, Pair<Double, SFNode>> schema2Engagements = new HashMap<>();
        initEngagementsSchemas(ipg, schema1Engagements, schema2Engagements);

        boolean change = true;
        while (change) {
            change = false;

            for (Entry<SFNode, Pair<SFNode, List<Pair<Double, SFNode>>>> entry : schema1Engagements.entrySet()) {
                if (entry.getValue().getFirst() == null && entry.getValue().getSecond().size() > 0) {
                    List<Pair<Double, SFNode>> possibleMarriages = entry.getValue().getSecond();
                    Pair<Double, SFNode> proposal = possibleMarriages.remove(0);

                    if (schema2Engagements.get(proposal.getSecond()) == null) {
                        schema2Engagements.put(proposal.getSecond(), new Pair<>(proposal.getFirst(), entry.getKey()));
                        entry.setValue(new Pair<>(proposal.getSecond(), entry.getValue().getSecond()));
                    } else {
                        if (schema2Engagements.get(proposal.getSecond()).getFirst() < proposal.getFirst()) {
                            Pair<SFNode, List<Pair<Double, SFNode>>> oldPair = schema1Engagements.get(schema2Engagements.get(proposal.getSecond()).getSecond());
                            schema1Engagements.put(schema2Engagements.get(proposal.getSecond()).getSecond(), new Pair<>(null, oldPair.getSecond()));
                            schema2Engagements.put(proposal.getSecond(), new Pair<>(proposal.getFirst(), entry.getKey()));
                            entry.setValue(new Pair<>(proposal.getSecond(), entry.getValue().getSecond()));
                        }
                    }
                    change = true;
                }
            }
        }

        for (Entry<SFNode, Pair<SFNode, List<Pair<Double, SFNode>>>> entry : schema1Engagements.entrySet()) {
            engagements.add(new Pair<>(new Pair<>(entry.getKey(), entry.getValue().getFirst()), 0.0));
        }

        for (Entry<SFNode, Pair<Double, SFNode>> entry : schema2Engagements.entrySet()) {
            if (entry.getValue() == null) {
                engagements.add(new Pair<>(new Pair<>(null, entry.getKey()), 0.0));
            }
        }

        return engagements;
    }

    private void initEngagementsSchemas(SimpleDirectedGraph<IPGNode, CoeffEdge> ipg, HashMap<SFNode, Pair<SFNode, List<Pair<Double, SFNode>>>> schema1Engagements,
        HashMap<SFNode, Pair<Double, SFNode>> schema2Engagements) {
        HashMap<SFNode, HashMap<SFNode, Double>> schema = clearSfCompressed(ipg);

        for (Entry<SFNode, HashMap<SFNode, Double>> nodeAEntry : schema.entrySet()) {
            SFNode nodeA = nodeAEntry.getKey();

            for (Entry<SFNode, Double> nodeBEntry : nodeAEntry.getValue().entrySet()) {
                SFNode nodeB = nodeBEntry.getKey();

                if (!schema1Engagements.containsKey(nodeA)) {
                    schema1Engagements.put(nodeA, new Pair<>(null, new ArrayList<>()));
                }
                schema1Engagements.get(nodeA).getSecond().add(new Pair<>(schema.get(nodeA).get(nodeB), nodeB));
                if (!schema2Engagements.containsKey(nodeB)) {
                    schema2Engagements.put(nodeB, null);
                }
            }
        }

        for (Pair<SFNode, List<Pair<Double, SFNode>>> l : schema1Engagements.values()) {
            l.getSecond().sort(Comparator.<Pair<Double, SFNode>>comparingDouble(Pair::getFirst).reversed());
        }
    }

    private HashMap<SFNode, HashMap<SFNode, Double>> clearSfCompressed(SimpleDirectedGraph<IPGNode, CoeffEdge> ipg) {
        HashMap<SFNode, HashMap<SFNode, Double>> nodeSimMap = new HashMap<>();
        for (IPGNode node : ipg.vertexSet()) {
            SFNode nodeA = node.getPairwiseConnectivityNode().getA();
            SFNode nodeB = node.getPairwiseConnectivityNode().getB();
            if (node.getCurrSim() > minSim && nodeA.getType().equals(SFNodeType.LITERAL) && nodeB.getType().equals(SFNodeType.LITERAL)) {
                if (!nodeSimMap.containsKey(nodeA)) {
                    nodeSimMap.put(nodeA, new HashMap<>());
                }
                nodeSimMap.get(nodeA).put(nodeB, node.getCurrSim());
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
            for (LabeledEdge edgeFromB : schemaGraphB.edgeSet()) {
                if (edgeFromA.getType().equals(edgeFromB.getType())) {

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

                    pcg.addEdge(sourceNode, targetNode, new LabeledEdge(edgeFromA.getType()));
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

            CoeffEdge coeffEdgeA = new CoeffEdge(inverseProduct(pcg.getEdgeSource(pcgEdge), pcg).get(pcgEdge.getType()));
            ipg.addEdge(nodeA, nodeB, coeffEdgeA);

            CoeffEdge coeffEdgeB = new CoeffEdge(inverseProduct(pcg.getEdgeTarget(pcgEdge), pcg).get(pcgEdge.getType()));
            ipg.addEdge(nodeB, nodeA, coeffEdgeB);
        }
        return ipg;
    }

    SimpleDirectedGraph<SFNode, LabeledEdge> createGraphForSchema(SimilarityFloodingSchema schema, HashMap<String, MatchableTableColumn> schemaMap) {

        SimpleDirectedGraph<SFNode, LabeledEdge> graph = new SimpleDirectedGraph<>(LabeledEdge.class);
        String currOid = "&1";

        // prepare Table type node
        SFNode tableOid = createTableNodes(schema, graph, currOid);

        // prepare Column type node
        SFNode columnType = new SFNode("Column", SFNodeType.IDENTIFIER);
        graph.addVertex(columnType);

        // prepare ColumnType type node
        SFNode columnTypeTypeNode = new SFNode("ColumnType", SFNodeType.IDENTIFIER);
        graph.addVertex(columnTypeTypeNode);

        // prepare data type nodes but add only if datatype exists
        HashMap<DataType, Pair<SFNode, SFNode>> dateTypeMap = new HashMap<>();
        dateTypeMap.put(DataType.string, new Pair<>(new SFNode("string", SFNodeType.LITERAL), null));
        dateTypeMap.put(DataType.numeric, new Pair<>(new SFNode("numeric", SFNodeType.LITERAL), null));
        dateTypeMap.put(DataType.date, new Pair<>(new SFNode("date", SFNodeType.LITERAL), null));

        for (MatchableTableColumn column : schema.getColumns()) {
            SFNode columnName = new SFNode(column.getHeader(), SFNodeType.LITERAL);
            schemaMap.put(column.getHeader(), column);
            graph.addVertex(columnName);

            currOid = nextOid(currOid);
            SFNode columnOid = new SFNode(currOid, SFNodeType.IDENTIFIER);
            graph.addVertex(columnOid);

            graph.addEdge(tableOid, columnOid, new LabeledEdge(LabeledEdgeType.COLUMN));
            graph.addEdge(columnOid, columnName, new LabeledEdge(LabeledEdgeType.NAME));
            graph.addEdge(columnOid, columnType, new LabeledEdge(LabeledEdgeType.TYPE));

            // dataType edge
            Pair<SFNode, SFNode> pair = dateTypeMap.get(column.getType());
            SFNode typeName = pair.getFirst();
            SFNode typeOid = pair.getSecond();

            if (typeOid == null) {
                currOid = nextOid(currOid);
                typeOid = new SFNode(currOid, SFNodeType.IDENTIFIER);
                dateTypeMap.put(column.getType(), new Pair<>(typeName, typeOid));
                graph.addVertex(typeName);
                graph.addVertex(typeOid);

                graph.addEdge(typeOid, typeName, new LabeledEdge(LabeledEdgeType.NAME));
                graph.addEdge(typeOid, columnTypeTypeNode, new LabeledEdge(LabeledEdgeType.TYPE));
            }
            graph.addEdge(columnOid, typeOid, new LabeledEdge(LabeledEdgeType.DATA_TYPE));
        }
        return graph;
    }

    private SFNode createTableNodes(SimilarityFloodingSchema schema, SimpleDirectedGraph<SFNode, LabeledEdge> graph, String currOid) {
        SFNode tableTypeNode = new SFNode("Table", SFNodeType.IDENTIFIER);
        graph.addVertex(tableTypeNode);

        // Node for table metadata
        SFNode tableOidNode = new SFNode(currOid, SFNodeType.IDENTIFIER);
        graph.addVertex(tableOidNode);

        SFNode tableNameNode = new SFNode(schema.getTableName(), SFNodeType.LITERAL);
        graph.addVertex(tableNameNode);

        // type edge
        graph.addEdge(tableOidNode, tableTypeNode, new LabeledEdge(LabeledEdgeType.TYPE));

        // name edge
        graph.addEdge(tableNameNode, tableOidNode, new LabeledEdge(LabeledEdgeType.NAME));
        return tableOidNode;
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

    private HashMap<LabeledEdgeType, Double> inverseProduct(PairwiseConnectivityNode node, SimpleDirectedGraph<PairwiseConnectivityNode, LabeledEdge> pcd) {
        HashMap<LabeledEdgeType, List<PairwiseConnectivityNode>> nodeByLabels = partitionNeighboursByLabels(node, pcd);

        HashMap<LabeledEdgeType, Double> result = new HashMap<>();
        for (Entry<LabeledEdgeType, List<PairwiseConnectivityNode>> label : nodeByLabels.entrySet()) {
            result.put(label.getKey(), 1 / (double) label.getValue().size());
        }

        return result;
    }


    private HashMap<LabeledEdgeType, List<PairwiseConnectivityNode>> partitionNeighboursByLabels(PairwiseConnectivityNode node, SimpleDirectedGraph<PairwiseConnectivityNode, LabeledEdge> pcd) {
        HashMap<LabeledEdgeType, List<PairwiseConnectivityNode>> neighboursByEdgeType = new HashMap<>();

        for (LabeledEdge outgoingEdge : pcd.outgoingEdgesOf(node)) {
            if (!neighboursByEdgeType.containsKey(outgoingEdge.getType())) {
                neighboursByEdgeType.put(outgoingEdge.getType(), new ArrayList<>());
            }
            neighboursByEdgeType.get(outgoingEdge.getType()).add(pcd.getEdgeTarget(outgoingEdge));
        }

        for (LabeledEdge incomingEdge : pcd.incomingEdgesOf(node)) {
            if (!neighboursByEdgeType.containsKey(incomingEdge.getType())) {
                neighboursByEdgeType.put(incomingEdge.getType(), new ArrayList<>());
            }
            neighboursByEdgeType.get(incomingEdge.getType()).add(pcd.getEdgeSource(incomingEdge));
        }

        return neighboursByEdgeType;
    }

    private HashMap<SFNode, HashMap<SFNode, Double>> generateInitialMap(SimpleDirectedGraph<SFNode, LabeledEdge> schemaGraphA, SimpleDirectedGraph<SFNode, LabeledEdge> schemaGraphB) {
        HashMap<SFNode, HashMap<SFNode, Double>> simMap = new HashMap<>();
        LevenshteinSimilarity levenshtein = new LevenshteinSimilarity();

        for (SFNode nodeFromA : schemaGraphA.vertexSet()) {
            for (SFNode nodeFromB : schemaGraphB.vertexSet()) {

                double sim;
                if (nodeFromA.getValue().equals("string") && nodeFromB.getValue().equals("numeric") || nodeFromA.getValue().equals("numeric") && nodeFromB.getValue().equals("string")) {
                    sim = 0.0001;
                } else {
                    sim = levenshtein.calculate(nodeFromA.getValue(), nodeFromB.getValue());
                }

                if (!simMap.containsKey(nodeFromA)) {
                    simMap.put(nodeFromA, new HashMap<>());
                }

                simMap.get(nodeFromA).put(nodeFromB, sim);
            }
        }
        return simMap;
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

    public double getMinSim() {
        return minSim;
    }

    public void setMinSim(double minSim) {
        this.minSim = minSim;
    }

    @Override
    public Processable<Correspondence<MatchableTableColumn, MatchableTableColumn>> getResult() {
        return result;
    }
}
