package de.uni_mannheim.informatik.dws.winter.matching.algorithms.sf;

import de.uni_mannheim.informatik.dws.winter.webtables.MatchableTableColumn;
import java.util.List;

public class SimilarityFloodingSchema {

    private String tableName;
    private List<MatchableTableColumn> columns;

    public SimilarityFloodingSchema(String tableName, List<MatchableTableColumn> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<MatchableTableColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<MatchableTableColumn> columns) {
        this.columns = columns;
    }
}
