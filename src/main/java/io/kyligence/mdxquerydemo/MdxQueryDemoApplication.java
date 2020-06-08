package io.kyligence.mdxquerydemo;

import org.olap4j.*;
import org.olap4j.metadata.Member;

import java.sql.Connection;
import java.sql.DriverManager;

public class MdxQueryDemoApplication {

    public static void main(String[] args) throws Exception {
        Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");
        Connection connection =
                DriverManager.getConnection(
                        "jdbc:xmla:Server=http://localhost:7080/mdx/xmla/learn_kylin?username=ADMIN&password=KYLIN");
        OlapConnection olapConnection = connection.unwrap(OlapConnection.class);
        OlapStatement statement = olapConnection.createStatement();
        String mdx = "SELECT NON EMPTY Hierarchize(AddCalculatedMembers({DrilldownLevel({[KYLIN_COUNTRY].[NAME].[All]})})) " +
                "DIMENSION PROPERTIES PARENT_UNIQUE_NAME ON COLUMNS  FROM [test001] WHERE ([Measures].[SUM_PRICE]) " +
                "CELL PROPERTIES VALUE, FORMAT_STRING, LANGUAGE, BACK_COLOR, FORE_COLOR, FONT_FLAGS";
        CellSet cellSet =
                statement.executeOlapQuery(mdx);
        for (Position column : cellSet.getAxes().get(0)) {
            for (Member member : column.getMembers()) {
                System.out.println(member.getUniqueName());
            }
            final Cell cell = cellSet.getCell(column);
            System.out.println(cell.getValue());
            System.out.println();
        }
    }



}
