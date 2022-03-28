package io.kyligence.mdxquerydemo;

import org.olap4j.*;
import org.olap4j.metadata.Member;

import java.sql.Connection;
import java.sql.DriverManager;

public class MdxQueryDemoApplication {

    public static void main(String[] args) throws Exception {
        String mdx_ip = args.length != 0 ? args[0] : "localhost";
        Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");
        Connection connection =
                DriverManager.getConnection(
                        "jdbc:xmla:Server=http://" + mdx_ip + ":7080/mdx/xmla/test_covid_project?username=ADMIN&password=KYLIN");
        OlapConnection olapConnection = connection.unwrap(OlapConnection.class);
        OlapStatement statement = olapConnection.createStatement();
        String mdx = "SELECT\n" +
                "{[Measures].DefaultMember} DIMENSION PROPERTIES [MEMBER_UNIQUE_NAME],[MEMBER_ORDINAL],[MEMBER_CAPTION] ON COLUMNS,\n" +
                "NON EMPTY CROSSJOIN(\n" +
                "  [DROPOFF_NEWYORK_ZONE].[BOROUGH].[BOROUGH].AllMembers,\n" +
                "  [PICKUP_NEWYORK_ZONE].[BOROUGH].[BOROUGH].AllMembers) DIMENSION PROPERTIES [MEMBER_UNIQUE_NAME],[MEMBER_ORDINAL],[MEMBER_CAPTION] ON ROWS\n" +
                "FROM [nyc_taxi_coivd_dataset]";
        CellSet cellSet =
                statement.executeOlapQuery(mdx);

        boolean flag = false;
        for (Position row : cellSet.getAxes().get(1)) {
            for (Position column : cellSet.getAxes().get(0)) {
                if (!flag) {
                    flag = true;
                    for (Member member : row.getMembers()) {
                        String name = member.getUniqueName();
                        String[] s = name.split("\\.");
                        if (s.length > 1) {
                            name = s[0].concat(s[1]);
                        }
                        System.out.print(name);
                        System.out.print("  ");
                    }
                    for (Member member : column.getMembers()) {
                        System.out.println(member.getUniqueName());
                        System.out.println("-------------------------------------------------------------------------");
                    }
                }
                for (Member member : row.getMembers()) {
                    System.out.print(member.getCaption());
                    System.out.print("        ");
                }
                final Cell cell = cellSet.getCell(column, row);
                System.out.println(cell.getFormattedValue());
            }
        }
    }
}
