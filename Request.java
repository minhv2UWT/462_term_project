package lambda;

import java.util.List;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    private String bucketname;
    private String filename;
    private int row;
    private int col;
    private List<String> groupByColumns;  // List of columns to GROUP BY
    private String whereClause;  // Optional WHERE clause
    private String aggregationFunction;  // Aggregation function (SUM, AVG, etc.)
    String name;

    // Getter for groupByColumns
    public List<String> getGroupByColumns() {
        return groupByColumns;
    }

    // Setter for groupByColumns
    public void setGroupByColumns(List<String> groupByColumns) {
        this.groupByColumns = groupByColumns;
    }

    // Getter for whereClause
    public String getWhereClause() {
        return whereClause;
    }

    // Setter for whereClause
    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }

    // Getter for aggregationFunction
    public String getAggregationFunction() {
        return aggregationFunction;
    }

    // Setter for aggregationFunction
    public void setAggregationFunction(String aggregationFunction) {
        this.aggregationFunction = aggregationFunction;
    }

    public String getName() {
        return name;
    }

    public String getNameALLCAPS() {
        return name.toUpperCase();
    }

    public void setName(String name) {
        this.name = name;
    }

    public Request(String name) {
        this.name = name;
    }

    public Request() {

    }

    /**
     * @return the bucketname
     */
    public String getBucketname() {
        return bucketname;
    }

    /**
     * @param bucketname the bucketname to set
     */
    public void setBucketname(String bucketname) {
        this.bucketname = bucketname;
    }

    /**
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @param filename the filename to set
     */
    public void setFilename(String filename) {
        this.filename = filename;
    }

    /**
     * @return the row
     */
    public int getRow() {
        return row;
    }

    /**
     * @param row the row to set
     */
    public void setRow(int row) {
        this.row = row;
    }

    /**
     * @return the col
     */
    public int getCol() {
        return col;
    }

    /**
     * @param col the col to set
     */
    public void setCol(int col) {
        this.col = col;
    }
}
