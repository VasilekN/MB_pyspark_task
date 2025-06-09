# MB_pyspark_task

Test task is for PySpark DataFrame(pyspark.sql.DataFrame) with products, categories and their links. 
Each product can have several categories or none.
And each category can have several products or none.

All data is stored in data folder.

Main.py file outputs the following information:
- dataframe with pairs 'Product - Category' (get_product_category_pairs())
  - dataframe with all products without category (get_products_without_categories()).