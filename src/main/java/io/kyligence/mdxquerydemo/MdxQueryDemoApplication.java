package io.kyligence.mdxquerydemo;

import org.olap4j.*;
import org.olap4j.metadata.Member;

import java.sql.Connection;
import java.sql.DriverManager;

public class MdxQueryDemoApplication {

    public static void main(String[] args) throws Exception {
        String mdx_ip = args.length != 0 ? args[0] : "localhost";
        String mdx_query = args.length > 1 ? args[1] : "SELECT\n" +
                "{[Measures].[ORDER_COUNT],\n" +
                "[Measures].[trip_mean_distance]} \n" +
                "DIMENSION PROPERTIES [MEMBER_UNIQUE_NAME],[MEMBER_ORDINAL],[MEMBER_CAPTION] ON COLUMNS,\n" +
                "NON EMPTY [PICKUP_NEWYORK_ZONE].[BOROUGH].[BOROUGH].AllMembers \n" +
                "DIMENSION PROPERTIES [MEMBER_UNIQUE_NAME],[MEMBER_ORDINAL],[MEMBER_CAPTION] ON ROWS\n" +
                "FROM [covid_trip_dataset]";
        Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");
        Connection connection =
                DriverManager.getConnection(
                        "jdbc:xmla:Server=http://" + mdx_ip + ":7080/mdx/xmla/covid_trip_project?username=ADMIN&password=KYLIN");
        OlapConnection olapConnection = connection.unwrap(OlapConnection.class);
        OlapStatement statement = olapConnection.createStatement();
        CellSet cellSet =
                statement.executeOlapQuery(mdx_query);

        Boolean rowFlag = false;
        Boolean columnFlag = false;
        for (Position row : cellSet.getAxes().get(1)) {
            for (Member member : row.getMembers()) {
                String name = member.getUniqueName();
                String[] s = name.split("\\.");
                if (s.length > 1) {
                    name = s[0].concat(".").concat(s[1]);
                }
                if (!rowFlag) {
                    System.out.print(name);
                    System.out.print("  ");
                }
            }
            for (Position column : cellSet.getAxes().get(0)) {
                for (Member member : column.getMembers()) {
                    if (!columnFlag) {
                        System.out.print(member.getUniqueName());
                        System.out.print("  ");
                    }
                }
            }
            rowFlag = true;
            columnFlag = true;
        }
        System.out.println("  ");
        System.out.println("-------------------------------------------------------------------------");

        Boolean columnNameDiff = true;
        String lastColumnName = "";
        for (Position row : cellSet.getAxes().get(1)) {
            for (Position column : cellSet.getAxes().get(0)) {
                for (Member member : row.getMembers()) {
                    String columnName = member.getCaption();
                    columnNameDiff = !columnName.equals(lastColumnName);
                    if (columnNameDiff) {
                        System.out.print(columnName);
                        System.out.print("        ");
                    }
                    lastColumnName = columnName;
                }
                final Cell cell = cellSet.getCell(column, row);
                System.out.print(cell.getFormattedValue());
                System.out.print("        ");
                if (!columnNameDiff) {
                    System.out.println();
                }
            }
        }
    }
}
