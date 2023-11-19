/*
For agg tables with a large number of dimensions, it may not be prudent or possible to calculate every possible combination of rollups across those dimensions.

One common approach has been to create a matrix of zeros and ones, with each row defining which dimensions are rolled up or broken out into individual members. 

In the case below, 0 means "aggregate to the dimension member level", while 1 means "summarize the total across all dimension members". 
*/

SELECT 0 AS dimension1_flag, 0 AS dimension2_flag, 0 AS dimension3_flag, 0 AS dimension4_flag
UNION ALL
SELECT 1,                    0,                    0,                    0
UNION ALL
SELECT 1,                    1,                    0,                    0
UNION ALL
SELECT 1,                    1,                    1,                    1

/*
The above is fed into a loop where dynamic SQL is executed for each row.
*/

for v_dimension1_flag, v_dimension2_flag, v_dimension3_flag, v_dimension4_flag

loop
    
	SELECT 
	  CASE v_dimension1_flag WHEN 1 THEN 'Dimension 1 Total' WHEN 0 THEN dimension1 END AS dimension1,
		CASE v_dimension2_flag WHEN 1 THEN 'Dimension 2 Total' WHEN 0 THEN dimension2 END AS dimension2,
		CASE v_dimension3_flag WHEN 1 THEN 'Dimension 3 Total' WHEN 0 THEN dimension3 END AS dimension3,
		CASE v_dimension4_flag WHEN 1 THEN 'Dimension 4 Total' WHEN 0 THEN dimension1 END AS dimension4,
		SUM(measure1) AS measure1
	
	FROM
	    fact_table
    GROUP BY
	    1,2,3,4
		
end

/*
An alternative to running a new query for each set of coordinates would be to use your rollup matrix to generate grouping sets.
*/

SELECT
    ARRAY_TO_STRING(ARRAY_AGG(
    CASE LENGTH(grouping_set_str) WHEN 0 THEN NULL ELSE
    '(' || SUBSTR(grouping_set_str::varchar(10000), 0, LENGTH(grouping_set_str) -1) || ')'
    END), ',')::varchar(10000) grouping_set_def

INTO v_group_set_desc

FROM (
    SELECT  
	    CASE dimension1_flag WHEN 0 THEN 'dimenson1,' ELSE '' END ||
		CASE dimension2_flag WHEN 0 THEN 'dimenson2,' ELSE '' END ||
		CASE dimension3_flag WHEN 0 THEN 'dimenson3,' ELSE '' END ||
		CASE dimension4_flag WHEN 0 THEN 'dimenson4,' ELSE '' END 
        AS grouping_set_str
    FROM (
        SELECT 0 AS dimension1_flag, 0 AS dimension2_flag, 0 AS dimension3_flag, 0 AS dimension4_flag
        UNION ALL
        SELECT 1,                    0,                    0,                    0
        UNION ALL
        SELECT 1,                    1,                    0,                    0
        UNION ALL
        SELECT 1,                    1,                    1,                    1
		
		) coord
		
	) grouping_def 

/*	
With the query above producing something like:
*/
(dimenson1,dimenson2,dimenson3,dimenson),(dimenson2,dimenson3,dimenson),(dimenson3,dimenson)

/*
Using an aggreation query with grouping sets has the advantage of producing all your needed rollups in one query, using a method native to the database, 
with SQL that will likely be more readable than a giant concatenated string.
*/

SELECT 
    dimension1,
	dimension2,
	dimension3,
	dimension4,
	SUM(measure1) AS measure1
FROM 
    fact_table
GROUP BY
    GROUPING SETS ((), v_group_set_desc)
