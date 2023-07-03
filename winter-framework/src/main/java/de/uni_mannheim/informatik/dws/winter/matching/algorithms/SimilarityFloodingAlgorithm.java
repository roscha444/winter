package de.uni_mannheim.informatik.dws.winter.matching.algorithms;

import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.FixpointFormula;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.CoeffEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.IPGNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.LabeledEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.LabeledEdgeType;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.PairwiseConnectivityNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNodeType;
import de.uni_mannheim.informatik.dws.winter.matching.rules.comparators.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import de.uni_mannheim.informatik.dws.winter.webtables.SFMatchable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.slf4j.Logger;

/**
 * Implementation of a Similarity Flooding Algorithm for structured schema matching
 *
 * (Similarity Flooding: A Versatile Graph Matching Algorithm and its Application to Schema Matching)
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class SimilarityFloodingAlgorithm<TypeA extends SFMatchable, TypeB extends Matchable> implements MatchingAlgorithm<TypeA, TypeB> {

    private static final Logger logger = WinterLogManager.getLogger();
    private String schemaAName = "A";
    private final List<TypeA> schemaA;
    private String schemaBName = "B";
    private final List<TypeA> schemaB;
    private final Comparator<TypeA, TypeA> labelComparator;
    private SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg;
    private ProcessableCollection<Correspondence<TypeA, TypeB>> result;

    // Default values for SF
    private double epsilon = 0.00002;
    private int maxSteps = 1000;
    private double defaultSim = 0.01;
    private double minSim = 0.00001;
    private boolean removeOid = false;
    private boolean alternativeInc = false;
    private final FixpointFormula fixpointFormula;

    public SimilarityFloodingAlgorithm(String schemaAName, List<TypeA> schemaA, String schemaBName, List<TypeA> schemaB, Comparator<TypeA, TypeA> labelComparator, FixpointFormula fixpointFormula) {
        this.schemaAName = schemaAName;
        this.schemaA = schemaA;
        this.schemaBName = schemaBName;
        this.schemaB = schemaB;
        this.labelComparator = labelComparator;
        this.fixpointFormula = fixpointFormula;
    }

    public SimilarityFloodingAlgorithm(List<TypeA> schemaA, List<TypeA> schemaB, Comparator<TypeA, TypeA> labelComparator, FixpointFormula fixpointFormula) {
        this.schemaA = schemaA;
        this.schemaB = schemaB;
        this.labelComparator = labelComparator;
        this.fixpointFormula = fixpointFormula;
    }

    public void run() {
        SimpleDirectedGraph<SFNode<TypeA>, LabeledEdge> schemaGraphA = createGraphForSchema(schemaA, schemaAName);
        SimpleDirectedGraph<SFNode<TypeA>, LabeledEdge> schemaGraphB = createGraphForSchema(schemaB, schemaBName);

        if (removeOid) {
            removeOidFromGraph(schemaGraphA);
            removeOidFromGraph(schemaGraphB);
        }

        SimpleDirectedGraph<PairwiseConnectivityNode<TypeA>, LabeledEdge> pcg = generatePairwiseConnectivityGraph(schemaGraphA, schemaGraphB);
        ipg = generateInducedPropagationGraph(schemaGraphA, schemaGraphB, pcg);

        similarityFlooding(ipg, maxSteps);

        List<Pair<Pair<SFNode<TypeA>, SFNode<TypeA>>, Double>> engagements = filterStableMarriage(ipg);

        result = new ProcessableCollection<>();
        logger.trace("Final result after filter application:");
        for (Pair<Pair<SFNode<TypeA>, SFNode<TypeA>>, Double> entry : engagements) {
            Pair<SFNode<TypeA>, SFNode<TypeA>> pair = entry.getFirst();
            if (pair.getFirst() == null) {
                logger.trace("no_match_found");
            } else {
                logger.trace(pair.getFirst().toString());
            }
            logger.trace("< - " + entry.getSecond() + " - >");
            if (pair.getSecond() == null) {
                logger.trace("no_match_found" + "\n");
            } else {
                logger.trace(pair.getSecond().toString() + "\n");
            }

            // CHECK
            if (pair.getFirst() == null || pair.getSecond() == null || pair.getSecond().getMatchable() == null || pair.getFirst().getMatchable() == null) {
                continue;
            }

            result.add(new Correspondence<>(pair.getFirst().getMatchable(), pair.getSecond().getMatchable(), entry.getSecond()));
        }
    }

    private SimpleDirectedGraph<SFNode<TypeA>, LabeledEdge> removeOidFromGraph(SimpleDirectedGraph<SFNode<TypeA>, LabeledEdge> graph) {

        List<SFNode<TypeA>> vertexToRemove = new ArrayList<>();
        List<LabeledEdge> edgeToRemove = new ArrayList<>();

        for (SFNode<TypeA> node : graph.vertexSet()) {
            if (node.getType().equals(SFNodeType.IDENTIFIER)) {
                List<LabeledEdge> nameNodeList = graph.outgoingEdgesOf(node).stream().filter(x -> x.getType().equals(LabeledEdgeType.NAME)).collect(Collectors.toList());
                if (nameNodeList.size() > 0) {

                    SFNode<TypeA> nameNode = graph.getEdgeTarget(nameNodeList.get(0));

                    edgeToRemove.add(graph.getEdge(nameNode, nameNode));
                    vertexToRemove.add(nameNode);

                    node.setGetIdentifier(nameNode.getGetIdentifier());
                    node.setMatchable(nameNode.getMatchable());
                }
            }
        }

        for (LabeledEdge edge : edgeToRemove) {
            graph.removeEdge(edge);
        }

        for (SFNode<TypeA> node : vertexToRemove) {
            graph.removeVertex(node);
        }

        return graph;
    }

    private List<Pair<Pair<SFNode<TypeA>, SFNode<TypeA>>, Double>> filterStableMarriage(SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg) {
        List<Pair<Pair<SFNode<TypeA>, SFNode<TypeA>>, Double>> engagements = new ArrayList<>();

        HashMap<SFNode<TypeA>, Pair<Pair<Double, SFNode<TypeA>>, List<Pair<Double, SFNode<TypeA>>>>> schema1Engagements = new HashMap<>();
        HashMap<SFNode<TypeA>, Pair<Double, SFNode<TypeA>>> schema2Engagements = new HashMap<>();
        initEngagementsSchemas(ipg, schema1Engagements, schema2Engagements);

        boolean change = true;
        while (change) {
            change = false;

            for (Entry<SFNode<TypeA>, Pair<Pair<Double, SFNode<TypeA>>, List<Pair<Double, SFNode<TypeA>>>>> entry : schema1Engagements.entrySet()) {
                if (entry.getValue().getFirst() == null && entry.getValue().getSecond().size() > 0) {
                    List<Pair<Double, SFNode<TypeA>>> possibleMarriages = entry.getValue().getSecond();
                    Pair<Double, SFNode<TypeA>> proposal = possibleMarriages.remove(0);

                    if (schema2Engagements.get(proposal.getSecond()) == null) {
                        schema2Engagements.put(proposal.getSecond(), new Pair<>(proposal.getFirst(), entry.getKey()));
                        entry.setValue(new Pair<>(proposal, entry.getValue().getSecond()));
                    } else {
                        if (schema2Engagements.get(proposal.getSecond()).getFirst() < proposal.getFirst()) {
                            Pair<Pair<Double, SFNode<TypeA>>, List<Pair<Double, SFNode<TypeA>>>> oldPair = schema1Engagements.get(schema2Engagements.get(proposal.getSecond()).getSecond());
                            schema1Engagements.put(schema2Engagements.get(proposal.getSecond()).getSecond(), new Pair<>(null, oldPair.getSecond()));
                            schema2Engagements.put(proposal.getSecond(), new Pair<>(proposal.getFirst(), entry.getKey()));
                            entry.setValue(new Pair<>(proposal, entry.getValue().getSecond()));
                        }
                    }
                    change = true;
                }
            }
        }

        for (Entry<SFNode<TypeA>, Pair<Pair<Double, SFNode<TypeA>>, List<Pair<Double, SFNode<TypeA>>>>> entry : schema1Engagements.entrySet()) {
            SFNode<TypeA> node = entry.getKey();
            Pair<Double, SFNode<TypeA>> bestMatch = entry.getValue().getFirst();
            double bestMatchSim = 0.0;
            SFNode<TypeA> bestMatchNode = null;
            if (bestMatch != null) {
                bestMatchSim = bestMatch.getFirst();
                bestMatchNode = bestMatch.getSecond();
            }
            engagements.add(new Pair<>(new Pair<>(node, bestMatchNode), bestMatchSim));
        }

        for (Entry<SFNode<TypeA>, Pair<Double, SFNode<TypeA>>> entry : schema2Engagements.entrySet()) {
            if (entry.getValue() == null) {
                engagements.add(new Pair<>(new Pair<>(null, entry.getKey()), 0.0));
            }
        }

        return engagements;
    }

    private void initEngagementsSchemas(SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg,
        HashMap<SFNode<TypeA>, Pair<Pair<Double, SFNode<TypeA>>, List<Pair<Double, SFNode<TypeA>>>>> schema1Engagements,
        HashMap<SFNode<TypeA>, Pair<Double, SFNode<TypeA>>> schema2Engagements) {
        HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> schema = clearSfCompressed(ipg);

        for (Entry<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> nodeAEntry : schema.entrySet()) {
            SFNode<TypeA> nodeA = nodeAEntry.getKey();

            for (Entry<SFNode<TypeA>, Double> nodeBEntry : nodeAEntry.getValue().entrySet()) {
                SFNode<TypeA> nodeB = nodeBEntry.getKey();

                if (!schema1Engagements.containsKey(nodeA)) {
                    schema1Engagements.put(nodeA, new Pair<>(null, new ArrayList<>()));
                }
                schema1Engagements.get(nodeA).getSecond().add(new Pair<>(schema.get(nodeA).get(nodeB), nodeB));
                if (!schema2Engagements.containsKey(nodeB)) {
                    schema2Engagements.put(nodeB, null);
                }
            }
        }

        for (Pair<Pair<Double, SFNode<TypeA>>, List<Pair<Double, SFNode<TypeA>>>> l : schema1Engagements.values()) {
            l.getSecond().sort(java.util.Comparator.<Pair<Double, SFNode<TypeA>>>comparingDouble(Pair::getFirst).reversed());
        }
    }

    private HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> clearSfCompressed(SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg) {
        HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> nodeSimMap = new HashMap<>();
        for (IPGNode<TypeA> node : ipg.vertexSet()) {
            SFNode<TypeA> nodeA = node.getPairwiseConnectivityNode().getA();
            SFNode<TypeA> nodeB = node.getPairwiseConnectivityNode().getB();
            if (node.getCurrSim() > minSim && (removeOid || nodeA.getType().equals(SFNodeType.LITERAL) && nodeB.getType().equals(SFNodeType.LITERAL))) {
                if (!nodeSimMap.containsKey(nodeA)) {
                    nodeSimMap.put(nodeA, new HashMap<>());
                }
                nodeSimMap.get(nodeA).put(nodeB, node.getCurrSim());
            }
        }
        return nodeSimMap;
    }

    void similarityFlooding(SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg, int maxSteps) {
        for (int i = 0; i < maxSteps; i++) {
            boolean cont = similarityFloodingStep(ipg);

            if (!cont) {
                logger.trace("Terminated: vector has length less than epsilon");
                break;
            }
        }
    }

    boolean similarityFloodingStep(SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg) {
        double maxSim = 0;
        double delta = 0;

        for (IPGNode<TypeA> node : ipg.vertexSet()) {

            double newSim;

            if (fixpointFormula.equals(FixpointFormula.A)) {
                newSim = fixpointA(node, ipg);
            } else if (fixpointFormula.equals(FixpointFormula.B)) {
                newSim = fixpointB(node, ipg);
            } else if (fixpointFormula.equals(FixpointFormula.C)) {
                newSim = fixpointC(node, ipg);
            } else {
                newSim = fixpointBasic(node, ipg);
            }

            maxSim = Math.max(maxSim, newSim);
            node.setNextSim(newSim);
        }

        for (IPGNode<TypeA> node : ipg.vertexSet()) {
            double newCurrSim = node.getNextSim() / maxSim;
            delta += Math.pow((node.getCurrSim() - newCurrSim), 2);
            node.setCurrSim(newCurrSim);
        }

        return Math.sqrt(delta) >= epsilon;
    }

    private double fixpointBasic(IPGNode<TypeA> node, SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg) {
        double increment = 0;

        for (CoeffEdge coeffEdge : ipg.incomingEdgesOf(node)) {
            increment += ipg.getEdgeSource(coeffEdge).getCurrSim() * coeffEdge.getCoeff();
        }

        return node.getCurrSim() + increment;
    }

    private double fixpointA(IPGNode<TypeA> node, SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg) {
        double increment = 0;

        for (CoeffEdge coeffEdge : ipg.incomingEdgesOf(node)) {
            increment += ipg.getEdgeSource(coeffEdge).getCurrSim() * coeffEdge.getCoeff();
        }

        return node.getInitSim() + increment;
    }

    private double fixpointB(IPGNode<TypeA> node, SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg) {
        double increment = 0;

        for (CoeffEdge coeffEdge : ipg.incomingEdgesOf(node)) {
            increment += (ipg.getEdgeSource(coeffEdge).getCurrSim() + ipg.getEdgeSource(coeffEdge).getInitSim()) * coeffEdge.getCoeff();
        }

        return increment;
    }

    private double fixpointC(IPGNode<TypeA> node, SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg) {
        double increment = 0;

        for (CoeffEdge coeffEdge : ipg.incomingEdgesOf(node)) {
            increment += (ipg.getEdgeSource(coeffEdge).getCurrSim() + ipg.getEdgeSource(coeffEdge).getInitSim()) * coeffEdge.getCoeff();
        }

        return node.getInitSim() + node.getCurrSim() + increment;
    }

    SimpleDirectedGraph<PairwiseConnectivityNode<TypeA>, LabeledEdge> generatePairwiseConnectivityGraph(SimpleDirectedGraph<SFNode<TypeA>, LabeledEdge> schemaGraphA,
        SimpleDirectedGraph<SFNode<TypeA>, LabeledEdge> schemaGraphB) {
        SimpleDirectedGraph<PairwiseConnectivityNode<TypeA>, LabeledEdge> pcg = new SimpleDirectedGraph<>(LabeledEdge.class);
        HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, PairwiseConnectivityNode<TypeA>>> nodeMap = new HashMap<>();
        for (LabeledEdge edgeFromA : schemaGraphA.edgeSet()) {
            for (LabeledEdge edgeFromB : schemaGraphB.edgeSet()) {
                if (edgeFromA.getType().equals(edgeFromB.getType())) {

                    PairwiseConnectivityNode<TypeA> sourceNode;
                    if (!nodeMap.containsKey(schemaGraphA.getEdgeSource(edgeFromA))) {
                        nodeMap.put(schemaGraphA.getEdgeSource(edgeFromA), new HashMap<>());
                    }
                    if (!nodeMap.get(schemaGraphA.getEdgeSource(edgeFromA)).containsKey(schemaGraphB.getEdgeSource(edgeFromB))) {
                        sourceNode = new PairwiseConnectivityNode<TypeA>(schemaGraphA.getEdgeSource(edgeFromA), schemaGraphB.getEdgeSource(edgeFromB));
                        nodeMap.get(schemaGraphA.getEdgeSource(edgeFromA)).put(schemaGraphB.getEdgeSource(edgeFromB), sourceNode);
                    } else {
                        sourceNode = nodeMap.get(schemaGraphA.getEdgeSource(edgeFromA)).get(schemaGraphB.getEdgeSource(edgeFromB));
                    }

                    PairwiseConnectivityNode<TypeA> targetNode;
                    if (!nodeMap.containsKey(schemaGraphA.getEdgeTarget(edgeFromA))) {
                        nodeMap.put(schemaGraphA.getEdgeTarget(edgeFromA), new HashMap<>());
                    }
                    if (!nodeMap.get(schemaGraphA.getEdgeTarget(edgeFromA)).containsKey(schemaGraphB.getEdgeTarget(edgeFromB))) {
                        targetNode = new PairwiseConnectivityNode<TypeA>(schemaGraphA.getEdgeTarget(edgeFromA), schemaGraphB.getEdgeTarget(edgeFromB));
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

    SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> generateInducedPropagationGraph(SimpleDirectedGraph<SFNode<TypeA>, LabeledEdge> schemaGraphA,
        SimpleDirectedGraph<SFNode<TypeA>, LabeledEdge> schemaGraphB,
        SimpleDirectedGraph<PairwiseConnectivityNode<TypeA>, LabeledEdge> pcg) {
        HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> simMap = generateInitialMap(schemaGraphA, schemaGraphB);
        SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg = new SimpleDirectedGraph<>(CoeffEdge.class);
        HashMap<PairwiseConnectivityNode<TypeA>, IPGNode<TypeA>> nodeMap = new HashMap<>();

        for (LabeledEdge pcgEdge : pcg.edgeSet()) {
            IPGNode<TypeA> nodeA = createIPGNodeFromPCGNode(pcg.getEdgeSource(pcgEdge), nodeMap, simMap);
            ipg.addVertex(nodeA);

            IPGNode<TypeA> nodeB = createIPGNodeFromPCGNode(pcg.getEdgeTarget(pcgEdge), nodeMap, simMap);
            ipg.addVertex(nodeB);

            CoeffEdge coeffEdgeA = new CoeffEdge(inverseProduct(pcg.getEdgeSource(pcgEdge), pcg).get(pcgEdge.getType()));
            ipg.addEdge(nodeA, nodeB, coeffEdgeA);

            CoeffEdge coeffEdgeB = new CoeffEdge(inverseProduct(pcg.getEdgeTarget(pcgEdge), pcg).get(pcgEdge.getType()));
            ipg.addEdge(nodeB, nodeA, coeffEdgeB);
        }
        return ipg;
    }

    SimpleDirectedGraph<SFNode<TypeA>, LabeledEdge> createGraphForSchema(List<TypeA> schema, String schemaName) {

        SimpleDirectedGraph<SFNode<TypeA>, LabeledEdge> graph = new SimpleDirectedGraph<>(LabeledEdge.class);
        String currOid = "&1";

        // prepare Table type node
        SFNode<TypeA> tableOid = createTableNodes(schemaName, graph, currOid);

        // prepare Column type node
        SFNode<TypeA> columnType = new SFNode<TypeA>("Column", SFNodeType.IDENTIFIER);
        graph.addVertex(columnType);

        // prepare ColumnType type node
        SFNode<TypeA> columnTypeTypeNode = new SFNode<TypeA>("ColumnType", SFNodeType.IDENTIFIER);
        graph.addVertex(columnTypeTypeNode);

        // prepare data type nodes but add only if datatype exists
        HashMap<DataType, Pair<SFNode<TypeA>, SFNode<TypeA>>> dateTypeMap = new HashMap<>();
        dateTypeMap.put(DataType.string, new Pair<>(new SFNode<TypeA>("string", SFNodeType.LITERAL), null));
        dateTypeMap.put(DataType.numeric, new Pair<>(new SFNode<TypeA>("numeric", SFNodeType.LITERAL), null));
        dateTypeMap.put(DataType.date, new Pair<>(new SFNode<TypeA>("date", SFNodeType.LITERAL), null));
        dateTypeMap.put(DataType.link, new Pair<>(new SFNode<TypeA>("link", SFNodeType.LITERAL), null));
        dateTypeMap.put(DataType.bool, new Pair<>(new SFNode<TypeA>("bool", SFNodeType.LITERAL), null));
        dateTypeMap.put(DataType.unit, new Pair<>(new SFNode<TypeA>("unit", SFNodeType.LITERAL), null));
        dateTypeMap.put(DataType.coordinate, new Pair<>(new SFNode<TypeA>("coordinate", SFNodeType.LITERAL), null));
        dateTypeMap.put(DataType.list, new Pair<>(new SFNode<TypeA>("list", SFNodeType.LITERAL), null));
        dateTypeMap.put(DataType.unknown, new Pair<>(new SFNode<TypeA>("unknown", SFNodeType.LITERAL), null));

        for (TypeA column : schema) {
            SFNode<TypeA> columnName = new SFNode<TypeA>(column.getIdentifier(), SFNodeType.LITERAL, column);
            graph.addVertex(columnName);

            currOid = nextOid(currOid);
            SFNode<TypeA> columnOid = new SFNode<TypeA>(currOid, SFNodeType.IDENTIFIER);
            graph.addVertex(columnOid);

            graph.addEdge(tableOid, columnOid, new LabeledEdge(LabeledEdgeType.COLUMN));
            graph.addEdge(columnOid, columnName, new LabeledEdge(LabeledEdgeType.NAME));
            graph.addEdge(columnOid, columnType, new LabeledEdge(LabeledEdgeType.TYPE));

            // dataType edge
            Pair<SFNode<TypeA>, SFNode<TypeA>> pair = dateTypeMap.get(column.getType());
            SFNode<TypeA> typeName = pair.getFirst();
            SFNode<TypeA> typeOid = pair.getSecond();

            if (typeOid == null) {
                currOid = nextOid(currOid);
                typeOid = new SFNode<TypeA>(currOid, SFNodeType.IDENTIFIER);
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

    private SFNode<TypeA> createTableNodes(String schemaName, SimpleDirectedGraph<SFNode<TypeA>, LabeledEdge> graph, String currOid) {
        SFNode<TypeA> tableTypeNode = new SFNode<TypeA>("Table", SFNodeType.IDENTIFIER);
        graph.addVertex(tableTypeNode);

        // Node for table metadata
        SFNode<TypeA> tableOidNode = new SFNode<TypeA>(currOid, SFNodeType.IDENTIFIER);
        graph.addVertex(tableOidNode);

        SFNode<TypeA> tableNameNode = new SFNode<TypeA>(schemaName, SFNodeType.LITERAL);
        graph.addVertex(tableNameNode);

        // type edge
        graph.addEdge(tableOidNode, tableTypeNode, new LabeledEdge(LabeledEdgeType.TYPE));

        // name edge
        graph.addEdge(tableOidNode, tableNameNode, new LabeledEdge(LabeledEdgeType.NAME));
        return tableOidNode;
    }

    private IPGNode<TypeA> createIPGNodeFromPCGNode(PairwiseConnectivityNode<TypeA> pcg, HashMap<PairwiseConnectivityNode<TypeA>, IPGNode<TypeA>> nodeMap,
        HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> simMap) {
        IPGNode<TypeA> ipgNode;

        if (!nodeMap.containsKey(pcg)) {
            double initSim;
            if (!simMap.containsKey(pcg.getA()) || simMap.containsKey(pcg.getA()) && !simMap.get(pcg.getA()).containsKey(pcg.getB())) {
                initSim = this.defaultSim;
            } else {
                initSim = simMap.get(pcg.getA()).get(pcg.getB());
            }
            ipgNode = new IPGNode<TypeA>(pcg, initSim, initSim, 0);
            nodeMap.put(pcg, ipgNode);
        } else {
            ipgNode = nodeMap.get(pcg);
        }
        return ipgNode;
    }

    private HashMap<LabeledEdgeType, Double> inverseProduct(PairwiseConnectivityNode<TypeA> node, SimpleDirectedGraph<PairwiseConnectivityNode<TypeA>, LabeledEdge> pcd) {
        HashMap<LabeledEdgeType, List<PairwiseConnectivityNode<TypeA>>> nodeByLabels = partitionNeighboursByLabels(node, pcd);

        HashMap<LabeledEdgeType, Double> result = new HashMap<>();
        for (Entry<LabeledEdgeType, List<PairwiseConnectivityNode<TypeA>>> label : nodeByLabels.entrySet()) {
            result.put(label.getKey(), 1 / (double) label.getValue().size());
        }

        return result;
    }


    private HashMap<LabeledEdgeType, List<PairwiseConnectivityNode<TypeA>>> partitionNeighboursByLabels(PairwiseConnectivityNode<TypeA> node,
        SimpleDirectedGraph<PairwiseConnectivityNode<TypeA>, LabeledEdge> pcd) {
        HashMap<LabeledEdgeType, List<PairwiseConnectivityNode<TypeA>>> neighboursByEdgeType = new HashMap<>();

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

    private HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> generateInitialMap(SimpleDirectedGraph<SFNode<TypeA>, LabeledEdge> schemaGraphA,
        SimpleDirectedGraph<SFNode<TypeA>, LabeledEdge> schemaGraphB) {
        HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> simMap = new HashMap<>();
        LevenshteinSimilarity levenshtein = new LevenshteinSimilarity();

        for (SFNode<TypeA> nodeFromA : schemaGraphA.vertexSet()) {
            for (SFNode<TypeA> nodeFromB : schemaGraphB.vertexSet()) {

                double sim;
                if (nodeFromA.getGetIdentifier().equals("string") && nodeFromB.getGetIdentifier().equals("string")
                    || nodeFromA.getGetIdentifier().equals("numeric") && nodeFromB.getGetIdentifier().equals("numeric")
                    || nodeFromA.getGetIdentifier().equals("unit") && nodeFromB.getGetIdentifier().equals("unit")
                    || nodeFromA.getGetIdentifier().equals("date") && nodeFromB.getGetIdentifier().equals("date")
                    || nodeFromA.getGetIdentifier().equals("bool") && nodeFromB.getGetIdentifier().equals("bool")
                    || nodeFromA.getGetIdentifier().equals("link") && nodeFromB.getGetIdentifier().equals("link")
                    || nodeFromA.getGetIdentifier().equals("coordinate") && nodeFromB.getGetIdentifier().equals("coordinate")
                    || nodeFromA.getGetIdentifier().equals("list") && nodeFromB.getGetIdentifier().equals("list")
                    || nodeFromA.getGetIdentifier().equals("unknown") && nodeFromB.getGetIdentifier().equals("unknown")
                ) {
                    sim = 0.5;
                } else if (
                    nodeFromA.getMatchable() == null || nodeFromB.getMatchable() == null

                        || nodeFromA.getGetIdentifier().equals("string") && nodeFromB.getGetIdentifier().equals("numeric")
                        || nodeFromA.getGetIdentifier().equals("string") && nodeFromB.getGetIdentifier().equals("date")
                        || nodeFromA.getGetIdentifier().equals("string") && nodeFromB.getGetIdentifier().equals("bool")
                        || nodeFromA.getGetIdentifier().equals("string") && nodeFromA.getGetIdentifier().equals("link")
                        || nodeFromA.getGetIdentifier().equals("string") && nodeFromA.getGetIdentifier().equals("unit")
                        || nodeFromA.getGetIdentifier().equals("string") && nodeFromA.getGetIdentifier().equals("unknown")
                        || nodeFromA.getGetIdentifier().equals("string") && nodeFromA.getGetIdentifier().equals("coordinate")
                        || nodeFromA.getGetIdentifier().equals("string") && nodeFromA.getGetIdentifier().equals("list")

                        || nodeFromA.getGetIdentifier().equals("numeric") && nodeFromA.getGetIdentifier().equals("unit")
                        || nodeFromA.getGetIdentifier().equals("numeric") && nodeFromB.getGetIdentifier().equals("string")
                        || nodeFromA.getGetIdentifier().equals("numeric") && nodeFromB.getGetIdentifier().equals("date")
                        || nodeFromA.getGetIdentifier().equals("numeric") && nodeFromB.getGetIdentifier().equals("bool")
                        || nodeFromA.getGetIdentifier().equals("numeric") && nodeFromB.getGetIdentifier().equals("link")
                        || nodeFromA.getGetIdentifier().equals("numeric") && nodeFromA.getGetIdentifier().equals("unknown")
                        || nodeFromA.getGetIdentifier().equals("numeric") && nodeFromA.getGetIdentifier().equals("coordinate")
                        || nodeFromA.getGetIdentifier().equals("numeric") && nodeFromA.getGetIdentifier().equals("list")

                        || nodeFromA.getGetIdentifier().equals("unit") && nodeFromA.getGetIdentifier().equals("numeric")
                        || nodeFromA.getGetIdentifier().equals("unit") && nodeFromB.getGetIdentifier().equals("string")
                        || nodeFromA.getGetIdentifier().equals("unit") && nodeFromB.getGetIdentifier().equals("date")
                        || nodeFromA.getGetIdentifier().equals("unit") && nodeFromB.getGetIdentifier().equals("bool")
                        || nodeFromA.getGetIdentifier().equals("unit") && nodeFromB.getGetIdentifier().equals("link")
                        || nodeFromA.getGetIdentifier().equals("unit") && nodeFromA.getGetIdentifier().equals("unknown")
                        || nodeFromA.getGetIdentifier().equals("unit") && nodeFromA.getGetIdentifier().equals("coordinate")
                        || nodeFromA.getGetIdentifier().equals("unit") && nodeFromA.getGetIdentifier().equals("list")

                        || nodeFromA.getGetIdentifier().equals("date") && nodeFromB.getGetIdentifier().equals("numeric")
                        || nodeFromA.getGetIdentifier().equals("date") && nodeFromB.getGetIdentifier().equals("string")
                        || nodeFromA.getGetIdentifier().equals("date") && nodeFromB.getGetIdentifier().equals("unit")
                        || nodeFromA.getGetIdentifier().equals("date") && nodeFromB.getGetIdentifier().equals("bool")
                        || nodeFromA.getGetIdentifier().equals("date") && nodeFromB.getGetIdentifier().equals("link")
                        || nodeFromA.getGetIdentifier().equals("date") && nodeFromA.getGetIdentifier().equals("unknown")
                        || nodeFromA.getGetIdentifier().equals("date") && nodeFromA.getGetIdentifier().equals("coordinate")
                        || nodeFromA.getGetIdentifier().equals("date") && nodeFromA.getGetIdentifier().equals("list")

                        || nodeFromA.getGetIdentifier().equals("bool") && nodeFromB.getGetIdentifier().equals("numeric")
                        || nodeFromA.getGetIdentifier().equals("bool") && nodeFromB.getGetIdentifier().equals("string")
                        || nodeFromA.getGetIdentifier().equals("bool") && nodeFromB.getGetIdentifier().equals("unit")
                        || nodeFromA.getGetIdentifier().equals("bool") && nodeFromB.getGetIdentifier().equals("date")
                        || nodeFromA.getGetIdentifier().equals("bool") && nodeFromB.getGetIdentifier().equals("link")
                        || nodeFromA.getGetIdentifier().equals("bool") && nodeFromA.getGetIdentifier().equals("unknown")
                        || nodeFromA.getGetIdentifier().equals("bool") && nodeFromA.getGetIdentifier().equals("coordinate")
                        || nodeFromA.getGetIdentifier().equals("bool") && nodeFromA.getGetIdentifier().equals("list")

                        || nodeFromA.getGetIdentifier().equals("unknown") && nodeFromB.getGetIdentifier().equals("string")
                        || nodeFromA.getGetIdentifier().equals("unknown") && nodeFromB.getGetIdentifier().equals("numeric")
                        || nodeFromA.getGetIdentifier().equals("unknown") && nodeFromB.getGetIdentifier().equals("unit")
                        || nodeFromA.getGetIdentifier().equals("unknown") && nodeFromB.getGetIdentifier().equals("date")
                        || nodeFromA.getGetIdentifier().equals("unknown") && nodeFromB.getGetIdentifier().equals("bool")
                        || nodeFromA.getGetIdentifier().equals("unknown") && nodeFromA.getGetIdentifier().equals("link")
                        || nodeFromA.getGetIdentifier().equals("unknown") && nodeFromA.getGetIdentifier().equals("coordinate")
                        || nodeFromA.getGetIdentifier().equals("unknown") && nodeFromA.getGetIdentifier().equals("list")

                        || nodeFromA.getGetIdentifier().equals("coordinate") && nodeFromB.getGetIdentifier().equals("string")
                        || nodeFromA.getGetIdentifier().equals("coordinate") && nodeFromB.getGetIdentifier().equals("numeric")
                        || nodeFromA.getGetIdentifier().equals("coordinate") && nodeFromB.getGetIdentifier().equals("unit")
                        || nodeFromA.getGetIdentifier().equals("coordinate") && nodeFromB.getGetIdentifier().equals("date")
                        || nodeFromA.getGetIdentifier().equals("coordinate") && nodeFromB.getGetIdentifier().equals("bool")
                        || nodeFromA.getGetIdentifier().equals("coordinate") && nodeFromA.getGetIdentifier().equals("unknown")
                        || nodeFromA.getGetIdentifier().equals("coordinate") && nodeFromA.getGetIdentifier().equals("link")
                        || nodeFromA.getGetIdentifier().equals("coordinate") && nodeFromA.getGetIdentifier().equals("list")

                        || nodeFromA.getGetIdentifier().equals("list") && nodeFromB.getGetIdentifier().equals("string")
                        || nodeFromA.getGetIdentifier().equals("list") && nodeFromB.getGetIdentifier().equals("numeric")
                        || nodeFromA.getGetIdentifier().equals("list") && nodeFromB.getGetIdentifier().equals("unit")
                        || nodeFromA.getGetIdentifier().equals("list") && nodeFromB.getGetIdentifier().equals("date")
                        || nodeFromA.getGetIdentifier().equals("list") && nodeFromB.getGetIdentifier().equals("bool")
                        || nodeFromA.getGetIdentifier().equals("list") && nodeFromA.getGetIdentifier().equals("unknown")
                        || nodeFromA.getGetIdentifier().equals("list") && nodeFromA.getGetIdentifier().equals("coordinate")
                        || nodeFromA.getGetIdentifier().equals("list") && nodeFromA.getGetIdentifier().equals("link")
                ) {
                    sim = defaultSim;
                } else {
                    sim = labelComparator.compare(nodeFromA.getMatchable(), nodeFromB.getMatchable(), null);
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

    public SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> getIpg() {
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

    public void setRemoveOid(boolean removeOid) {
        this.removeOid = removeOid;
    }

    @Override
    public Processable<Correspondence<TypeA, TypeB>> getResult() {
        return result;
    }
}
