# spark_sql_pet_project

There are two tak implementations:
- SQL: _org.company.sql_
- Programmatic: _org.company.programmatic_

To run jobs you can use:
- _EnrichSessionJobEntryPoint_ - corresponds to Task #1
- _MedianPerCategoryJobEntryPoint_ - corresponds to Task #2_A
- _RangingTimeSpentByUserPerCategory_ - corresponds to Task #2_B
- _TopTenProductsPerCategoryJobEntryPoint_ - corresponds to Task #2_C

Note that SQL and programmatic implementations has it's own entry points job in their corresponding packages.

Job input file can be changed using _dataFilePath_ in the: _org.company_

