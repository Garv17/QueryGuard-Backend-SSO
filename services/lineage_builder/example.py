from sql_lineage_builder import build_lineage
import logging
import json


def main() -> None:
    # logging.basicConfig(level=logging.DEBUG)
    
    # Test SQL lineage extraction
    print("=== SQL Lineage Extraction ===")
    extract_sql_lineage()


def extract_sql_lineage() -> None:
    """Extract SQL lineage showing both source-to-temp-view and temp-view-to-target mappings"""
    sql = """CREATE
OR
replace temporary VIEW prod_tz.edw_staging.generalledgersummary__dbt_tmp AS (WITH newco_yardi_gldetail_summary AS
(
       SELECT rowsourcesystem,
              accountcd,
              trim(yardipropertycode) AS yardipropertycode,
              perioddate,
              amount,
              cast(-(100 * bookid) AS smallint) AS bookid,
              bookname,
              scenariocd,
              deptcd,
              deptcd_lower,
              segment3
       FROM   prod_rz.yardi_dbo.gldetail6m), lookup_dimproducttype1_by_yardipropertycodenewco AS
(
       SELECT propertyhmy,
              unithmy,
              newcoyardipropertycode
       FROM   (
                       SELECT   row_number() OVER (partition BY newcoyardipropertycode ORDER BY propertyhmy ASC nulls last, unithmy ASC nulls last) AS rank,
                                *
                       FROM     (
                                       SELECT propertyspk                                                      AS propertyhmy,
                                              unitspk                                                          AS unithmy,
                                              trim(lower(COALESCE(yardipropertycodenewco, yardipropertycode))) AS newcoyardipropertycode
                                       FROM   prod_tz.edw.dimproducttype1
                                       WHERE  NOT unitspk IN (48715
                                                              /* Omit addition 2 unit for legacy IH 3 unit prop */
                                                              ,
                                                              48714)
                                       AND    COALESCE(reportingmarket, '') <> 'Test') AS u) AS b
       WHERE  rank = 1)SELECT     n.rowsourcesystem,
           n.accountcd,
           n.perioddate,
           n.amount AS tranamount,
           n.bookid,
           n.bookname,
           n.scenariocd,
           n.deptcd,
           n.segment3,
           l.propertyhmy,
           l.unithmy,
           n.yardipropertycode                              AS propertycode
FROM       newco_yardi_gldetail_summary                     AS n
INNER JOIN lookup_dimproducttype1_by_yardipropertycodenewco AS l
ON         n.yardipropertycode = l.newcoyardipropertycode)
/* {"app": "dbt", "dbt_version": "2025.7.24+00c7a0e", "profile_name": "user", "target_name": "prod", "node_id": "model.edw_migration.generalledgersummary"} */
;INSERT INTO prod_tz.edw_staging.generalledgersummary
            (
                        "propertyhmy",
                        "unithmy",
                        "perioddate",
                        "accountcd",
                        "scenariocd",
                        "deptcd",
                        "bookid",
                        "segment3",
                        "bookname",
                        "tranamount",
                        "rowsourcesystem",
                        "propertycode"
            )
            (
                   SELECT "propertyhmy",
                          "unithmy",
                          "perioddate",
                          "accountcd",
                          "scenariocd",
                          "deptcd",
                          "bookid",
                          "segment3",
                          "bookname",
                          "tranamount",
                          "rowsourcesystem",
                          "propertycode"
                   FROM   prod_tz.edw_staging.generalledgersummary__dbt_tmp)
            /* {"app": "dbt", "dbt_version": "2025.7.24+00c7a0e", "profile_name": "user", "target_name": "prod", "node_id": "model.edw_migration.generalledgersummary"} */"""

    lineage = build_lineage(sql, dialect="snowflake", enhanced_mode=True)

#     print(lineage)
    
    # Separate temp view <- source and target <- temp view mappings dynamically
    temp_view_mappings = []
    target_mappings = []
    
    for mapping in lineage.get("source_to_target", []):
        parts = mapping.split(" <- ")
        if len(parts) == 2:
            left_side = parts[0].lower()
            right_side = parts[1].lower()
            
            # Check if left side contains temp view pattern (__dbt_tmp) and right side doesn't
            # This indicates: temp_view <- source
            if "__dbt_tmp" in left_side and "__dbt_tmp" not in right_side:
                temp_view_mappings.append(mapping)
            # Check if right side contains temp view pattern (__dbt_tmp) and left side doesn't  
            # This indicates: target <- temp_view
            elif "__dbt_tmp" in right_side and "__dbt_tmp" not in left_side:
                target_mappings.append(mapping)
            else:
                # Fallback: if both or neither contain __dbt_tmp, classify based on position
                # First half of mappings are typically temp_view <- source
                # Second half are typically target <- temp_view
                if len(temp_view_mappings) < len(target_mappings):
                    temp_view_mappings.append(mapping)
                else:
                    target_mappings.append(mapping)
    
    # Build source_to_temp_view JSON
    source_to_temp_view = {}
    for mapping in temp_view_mappings:
        parts = mapping.split(" <- ")
        if len(parts) == 2:
            target_col = parts[0].lower()
            source_col = parts[1].lower()
            
            if target_col not in source_to_temp_view:
                source_to_temp_view[target_col] = []
            source_to_temp_view[target_col].append(source_col)
    
    # Build temp_view_to_target JSON
    temp_view_to_target = {}
    for mapping in target_mappings:
        parts = mapping.split(" <- ")
        if len(parts) == 2:
            target_col = parts[0].lower()
            source_col = parts[1].lower()
            
            if target_col not in temp_view_to_target:
                temp_view_to_target[target_col] = []
            temp_view_to_target[target_col].append(source_col)
    
    # Output in requested JSON format
#     print("source_to_temp_view :")
#     print(json.dumps(source_to_temp_view, indent=4))
#     print()
#     print("temp_view_to_target :")
#     print(json.dumps(temp_view_to_target, indent=4))

    final_lineage = {}

    for target, tmp_list in temp_view_to_target.items():
       sources = []
       for tmp in tmp_list:
          if tmp in source_to_temp_view:
              sources.extend(source_to_temp_view[tmp])
       # if no sources found, set to None
       final_lineage[target] = sources if sources else None

#     print(final_lineage)
    print(json.dumps(final_lineage, indent=2))




if __name__ == "__main__":
    main()


