---
layout: docs24
title:  Optimize Cube Design
categories: howto
permalink: /docs24/howto/howto_optimize_cubes.html
---

## Hierarchies:

Theoretically for N dimensions you'll end up with 2^N dimension combinations. However for some group of dimensions there are no need to create so many combinations. For example, if you have three dimensions: continent, country, city (In hierarchies, the "bigger" dimension comes first). You will only need the following three combinations of group by when you do drill down analysis:

group by continent
group by continent, country
group by continent, country, city

In such cases the combination count is reduced from 2^3=8 to 3, which is a great optimization. The same goes for the YEAR,QUATER,MONTH,DATE case.

If we Donate the hierarchy dimension as H1,H2,H3, typical scenarios would be:


A. Hierarchies on lookup table


<table>
  <tr>
    <td align="center">Fact table</td>
    <td align="center">(joins)</td>
    <td align="center">Lookup Table</td>
  </tr>
  <tr>
    <td>column1,column2,,,,,, FK</td>
    <td></td>
    <td>PK,,H1,H2,H3,,,,</td>
  </tr>
</table>

---

B. Hierarchies on fact table


<table>
  <tr>
    <td align="center">Fact table</td>
  </tr>
  <tr>
    <td>column1,column2,,,H1,H2,H3,,,,,,, </td>
  </tr>
</table>

---


There is a special case for scenario A, where PK on the lookup table is accidentally being part of the hierarchies. For example we have a calendar lookup table where cal_dt is the primary key:

A*. Hierarchies on lookup table over its primary key


<table>
  <tr>
    <td align="center">Lookup Table(Calendar)</td>
  </tr>
  <tr>
    <td>cal_dt(PK), week_beg_dt, month_beg_dt, quarter_beg_dt,,,</td>
  </tr>
</table>

---


For cases like A* what you need is another optimization called "Derived Columns"

## Derived Columns:

Derived column is used when one or more dimensions (They must be dimension on lookup table, these columns are called "Derived") can be deduced from another(Usually it is the corresponding FK, this is called the "host column")

For example, suppose we have a lookup table where we join fact table and it with "where DimA = DimX". Notice in Kylin, if you choose FK into a dimension, the corresponding PK will be automatically querable, without any extra cost. The secret is that since FK and PK are always identical, Kylin can apply filters/groupby on the FK first, and transparently replace them to PK.  This indicates that if we want the DimA(FK), DimX(PK), DimB, DimC in our cube, we can safely choose DimA,DimB,DimC only.

<table>
  <tr>
    <td align="center">Fact table</td>
    <td align="center">(joins)</td>
    <td align="center">Lookup Table</td>
  </tr>
  <tr>
    <td>column1,column2,,,,,, DimA(FK) </td>
    <td></td>
    <td>DimX(PK),,DimB, DimC</td>
  </tr>
</table>

---


Let's say that DimA(the dimension representing FK/PK) has a special mapping to DimB:


<table>
  <tr>
    <th>dimA</th>
    <th>dimB</th>
    <th>dimC</th>
  </tr>
  <tr>
    <td>1</td>
    <td>a</td>
    <td>?</td>
  </tr>
  <tr>
    <td>2</td>
    <td>b</td>
    <td>?</td>
  </tr>
  <tr>
    <td>3</td>
    <td>c</td>
    <td>?</td>
  </tr>
  <tr>
    <td>4</td>
    <td>a</td>
    <td>?</td>
  </tr>
</table>


in this case, given a value in DimA, the value of DimB is determined, so we say dimB can be derived from DimA. When we build a cube that contains both DimA and DimB, we simple include DimA, and marking DimB as derived. Derived column(DimB) does not participant in cuboids generation:

original combinations:
ABC,AB,AC,BC,A,B,C

combinations when driving B from A:
AC,A,C

at Runtime, in case queries like "select count(*) from fact_table inner join looup1 group by looup1 .dimB", it is expecting cuboid containing DimB to answer the query. However, DimB will appear in NONE of the cuboids due to derived optimization. In this case, we modify the execution plan to make it group by  DimA(its host column) first, we'll get intermediate answer like:


<table>
  <tr>
    <th>DimA</th>
    <th>count(*)</th>
  </tr>
  <tr>
    <td>1</td>
    <td>1</td>
  </tr>
  <tr>
    <td>2</td>
    <td>1</td>
  </tr>
  <tr>
    <td>3</td>
    <td>1</td>
  </tr>
  <tr>
    <td>4</td>
    <td>1</td>
  </tr>
</table>


Afterwards, Kylin will replace DimA values with DimB values(since both of their values are in lookup table, Kylin can load the whole lookup table into memory and build a mapping for them), and the intermediate result becomes:


<table>
  <tr>
    <th>DimB</th>
    <th>count(*)</th>
  </tr>
  <tr>
    <td>a</td>
    <td>1</td>
  </tr>
  <tr>
    <td>b</td>
    <td>1</td>
  </tr>
  <tr>
    <td>c</td>
    <td>1</td>
  </tr>
  <tr>
    <td>a</td>
    <td>1</td>
  </tr>
</table>


After this, the runtime SQL engine(calcite) will further aggregate the intermediate result to:


<table>
  <tr>
    <th>DimB</th>
    <th>count(*)</th>
  </tr>
  <tr>
    <td>a</td>
    <td>2</td>
  </tr>
  <tr>
    <td>b</td>
    <td>1</td>
  </tr>
  <tr>
    <td>c</td>
    <td>1</td>
  </tr>
</table>


this step happens at query runtime, this is what it means "at the cost of extra runtime aggregation"
