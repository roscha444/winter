package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.filter;

import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.CoeffEdge;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.ipg.IPGNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNode;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf.pcg.SFNodeType;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.jgrapht.graph.SimpleDirectedGraph;

/**
 * Hungarian Algorithm as SF filter
 *
 * Quelle: https://github.com/aalmi/HungarianAlgorithm/blob/master/HungarianAlgorithm.java#L53
 *
 * @author Robin Schumacher (info@robin-schumacher.com)
 */
public class HungarianAlgorithm<TypeA> extends Filter<TypeA> {

    private int[] zeroInRow;
    private int[] zeroInCol;
    private int[] colIsCovered;
    private int[] rowIsCovered;
    private int[] staredZeroesInRow;
    private int rowCount;
    private int colCount;
    private List<Pair<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>>> schemaAsArray = new ArrayList<>();


    public HungarianAlgorithm(double minSim, boolean removeOid) {
        super(minSim, removeOid);
    }

    @Override
    public List<Pair<Pair<SFNode<TypeA>, SFNode<TypeA>>, Double>> run(SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg) {
        prepareMatrix(ipg);

        rowCount = schemaAsArray.size();
        colCount = getColCount();

        zeroInRow = new int[rowCount];
        zeroInCol = new int[colCount];
        colIsCovered = new int[colCount];
        rowIsCovered = new int[rowCount];
        staredZeroesInRow = new int[rowCount];

        Arrays.fill(staredZeroesInRow, -1);
        Arrays.fill(zeroInRow, -1);
        Arrays.fill(zeroInCol, -1);

        double maxEntry = Double.MIN_VALUE;
        for (int colIndex = 0; colIndex < colCount; colIndex++) {
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Pair<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>> row = schemaAsArray.get(rowIndex);
                Double sim = row.getSecond().get(colIndex).getFirst();
                if (sim != placeHolderForMaxValue()) {
                    maxEntry = Math.max(sim, maxEntry);
                }
            }
        }

        for (int colIndex = 0; colIndex < colCount; colIndex++) {
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Pair<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>> row = schemaAsArray.get(rowIndex);
                double newSimilarity;
                if (row.getSecond().get(colIndex).getFirst() == placeHolderForMaxValue()) {
                    newSimilarity = maxEntry;
                } else {
                    newSimilarity = maxEntry - row.getSecond().get(colIndex).getFirst();
                }
                Pair<Double, SFNode<TypeA>> elem = new Pair<>(newSimilarity, row.getSecond().get(colIndex).getSecond());
                row.getSecond().set(colIndex, elem);
            }
        }

        reduceColumn();
        reduceRow();
        markZeroWithSquare();
        markCoveredColumn();

        while (!allColumnsAreCovered()) {
            int[] notCoveredZero = findNotCoveredZero();

            while (notCoveredZero == null) {
                applyMinUncoveredValueToMatrix();
                notCoveredZero = findNotCoveredZero();
            }

            if (isZeroInRow(notCoveredZero)) {
                // TODO
                crossZero(notCoveredZero);
                markCoveredColumn();
            } else {
                rowIsCovered[notCoveredZero[0]] = 1;
                colIsCovered[zeroInRow[notCoveredZero[0]]] = 0;
                applyMinUncoveredValueToMatrix();
            }
        }

        List<Pair<Pair<SFNode<TypeA>, SFNode<TypeA>>, Double>> result = new ArrayList<>();
        for (int col = 0; col < zeroInCol.length; col++) {
            int row = zeroInCol[col];
            result.add(new Pair<>(new Pair<>(schemaAsArray.get(row).getFirst(), schemaAsArray.get(row).getSecond().get(col).getSecond()), schemaAsArray.get(row).getSecond().get(col).getFirst()));
        }
        return result;
    }

    private static double placeHolderForMaxValue() {
        return 1111.11;
    }

    private boolean isZeroInRow(int[] notCoveredZero) {
        return zeroInRow[notCoveredZero[0]] == -1;
    }

    private void markCoveredColumn() {
        for (int i = 0; i < zeroInCol.length; i++) {
            colIsCovered[i] = zeroInCol[i] != -1 ? 1 : 0;
        }
    }

    private void markZeroWithSquare() {
        int[] rowHasZero = new int[rowCount];
        int[] colHasZero = new int[colCount];

        for (int colIndex = 0; colIndex < colCount; colIndex++) {
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                if (schemaAsArray.get(rowIndex).getSecond().get(colIndex).getFirst() == 0.0 && rowHasZero[rowIndex] == 0 && colHasZero[colIndex] == 0) {
                    rowHasZero[rowIndex] = 1;
                    colHasZero[colIndex] = 1;
                    zeroInRow[rowIndex] = colIndex;
                    zeroInCol[colIndex] = rowIndex;
                }
            }
        }
    }

    private int getColCount() {
        return schemaAsArray.stream().max(java.util.Comparator.<Pair<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>>>comparingDouble(x -> x.getSecond().size()).reversed())
            .orElse(new Pair<>(null, new ArrayList<>()))
            .getSecond().size();
    }

    private void prepareMatrix(SimpleDirectedGraph<IPGNode<TypeA>, CoeffEdge> ipg) {
        HashMap<SFNode<TypeA>, HashMap<SFNode<TypeA>, Double>> nodeSimMap = new HashMap<>();
        for (IPGNode<TypeA> node : ipg.vertexSet()) {
            SFNode<TypeA> nodeA = node.getPairwiseConnectivityNode().getA();
            SFNode<TypeA> nodeB = node.getPairwiseConnectivityNode().getB();

            // TODO
            if (nodeA.getMatchable() == null || nodeB.getMatchable() == null) {
                continue;
            }

            if (node.getCurrSim() > minSim && (removeOid || nodeA.getType().equals(SFNodeType.LITERAL) && nodeB.getType().equals(SFNodeType.LITERAL))) {
                if (!nodeSimMap.containsKey(nodeA)) {
                    nodeSimMap.put(nodeA, new HashMap<>());
                }
                nodeSimMap.get(nodeA).put(nodeB, node.getCurrSim());
                // TODO
            } else if (node.getCurrSim() <= minSim && removeOid) {
                if (!nodeSimMap.containsKey(nodeA)) {
                    nodeSimMap.put(nodeA, new HashMap<>());
                }
                nodeSimMap.get(nodeA).put(nodeB, placeHolderForMaxValue());
            }
        }

        for (Entry<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>> entry : getClearSfAsList(nodeSimMap).entrySet()) {
            schemaAsArray.add(new Pair<>(entry.getKey(), entry.getValue()));
        }

        rowCount = schemaAsArray.size();
        colCount = getColCount();
        int maxCount = Math.max(rowCount, colCount);

        if (schemaAsArray.size() < maxCount) {
            for (int i = schemaAsArray.size(); i < maxCount; i++) {
                List<Pair<Double, SFNode<TypeA>>> tmp = new ArrayList<>();
                for (Pair<Double, SFNode<TypeA>> pair : schemaAsArray.get(0).getSecond()) {
                    tmp.add(new Pair<>(placeHolderForMaxValue(), pair.getSecond()));
                }
                schemaAsArray.add(i, new Pair<>(schemaAsArray.get(0).getFirst(), tmp));
            }
        }
    }

    private void reduceColumn() {
        for (int colIndex = 0; colIndex < colCount; colIndex++) {
            double minColSimilarity = 0.0;

            for (Pair<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>> row : schemaAsArray) {

                double localMin = row.getSecond().get(colIndex).getFirst();
                if (localMin != 0.0) {
                    minColSimilarity = Math.min(minColSimilarity, localMin);
                }
            }

            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Pair<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>> row = schemaAsArray.get(rowIndex);
                double newSimilarity = row.getSecond().get(colIndex).getFirst() - minColSimilarity;
                Pair<Double, SFNode<TypeA>> elem = new Pair<>(newSimilarity, row.getSecond().get(colIndex).getSecond());
                row.getSecond().set(colIndex, elem);
            }
        }
    }

    private void reduceRow() {
        for (Pair<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>> row : schemaAsArray) {
            double minRowSimilarity = 0.0;

            for (int i = 0; i < colCount; i++) {
                double localMin = row.getSecond().get(i).getFirst();
                if (localMin != 0.0) {
                    minRowSimilarity = Math.min(minRowSimilarity, localMin);
                }
            }

            for (int i = 0; i < colCount; i++) {
                double newSimilarity = row.getSecond().get(i).getFirst() - minRowSimilarity;
                Pair<Double, SFNode<TypeA>> elem = new Pair<>(newSimilarity, row.getSecond().get(i).getSecond());
                row.getSecond().set(i, elem);
            }
        }
    }

    private int[] findNotCoveredZero() {
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            if (rowIsCovered[rowIndex] == 0) {
                for (int colIndex = 0; colIndex < colCount; colIndex++) {
                    if (schemaAsArray.get(rowIndex).getSecond().get(colIndex).getFirst() == 0.0 && colIsCovered[colIndex] == 0) {
                        staredZeroesInRow[rowIndex] = colIndex;
                        return new int[]{rowIndex, colIndex};
                    }
                }
            }
        }
        return null;
    }

    private void applyMinUncoveredValueToMatrix() {
        double minUncoveredValue = getSmallestUncoveredValue();
        if (minUncoveredValue > 0) {
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                for (int colIndex = 0; colIndex < colCount; colIndex++) {
                    if (rowIsCovered[rowIndex] == 1 && colIsCovered[colIndex] == 1) {

                        // Add min to all twice-covered values
                        Pair<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>> row = schemaAsArray.get(rowIndex);
                        double newSimilarity = row.getSecond().get(colIndex).getFirst() + minUncoveredValue;
                        Pair<Double, SFNode<TypeA>> elem = new Pair<>(newSimilarity, row.getSecond().get(colIndex).getSecond());
                        row.getSecond().set(colIndex, elem);

                    } else if (rowIsCovered[rowIndex] == 0 && colIsCovered[colIndex] == 0) {

                        // Subtract min from all uncovered values
                        Pair<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>> row = schemaAsArray.get(rowIndex);
                        double newSimilarity = row.getSecond().get(colIndex).getFirst() - minUncoveredValue;
                        Pair<Double, SFNode<TypeA>> elem = new Pair<>(newSimilarity, row.getSecond().get(colIndex).getSecond());
                        row.getSecond().set(colIndex, elem);

                    }
                }
            }
        }
    }

    private double getSmallestUncoveredValue() {
        double minUncoveredValue = Double.MAX_VALUE;
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            if (rowIsCovered[rowIndex] == 1) {
                continue;
            }
            for (int colIndex = 0; colIndex < colCount; colIndex++) {
                Pair<SFNode<TypeA>, List<Pair<Double, SFNode<TypeA>>>> row = schemaAsArray.get(rowIndex);
                if (colIsCovered[colIndex] == 0 && row.getSecond().get(colIndex).getFirst() < minUncoveredValue) {
                    minUncoveredValue = row.getSecond().get(colIndex).getFirst();
                }
            }
        }
        return minUncoveredValue;
    }

    private boolean allColumnsAreCovered() {
        for (int i : colIsCovered) {
            if (i == 0) {
                return false;
            }
        }
        return true;
    }

    private void crossZero(int[] mainZero) {
        int i = mainZero[0];
        int j = mainZero[1];

        Set<int[]> K = new LinkedHashSet<>();
        K.add(mainZero);
        boolean found = false;
        do {
            if (zeroInCol[j] != -1) {
                K.add(new int[]{zeroInCol[j], j});
                found = true;
            } else {
                found = false;
            }

            if (!found) {
                break;
            }

            i = zeroInCol[j];
            j = staredZeroesInRow[i];
            if (j != -1) {
                K.add(new int[]{i, j});
                found = true;
            } else {
                found = false;
            }

        } while (found);

        for (int[] zero : K) {
            if (zeroInCol[zero[1]] == zero[0]) {
                zeroInCol[zero[1]] = -1;
                zeroInRow[zero[0]] = -1;
            }
            if (staredZeroesInRow[zero[0]] == zero[1]) {
                zeroInRow[zero[0]] = zero[1];
                zeroInCol[zero[1]] = zero[0];
            }
        }

        Arrays.fill(staredZeroesInRow, -1);
        Arrays.fill(rowIsCovered, 0);
        Arrays.fill(colIsCovered, 0);
    }
}
