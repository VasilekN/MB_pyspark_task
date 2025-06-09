from pyspark.sql import SparkSession
from utils import get_product_category_pairs, get_products_without_categories

def main():
    spark = SparkSession.builder.appName("ProductCategoryAnalysis").getOrCreate()

    products_df = spark.read.option("header", True).csv("../data/products.csv")
    categories_df = spark.read.option("header", True).csv("../data/categories.csv")
    prod_cat_df = spark.read.option("header", True).csv("../data/product_category.csv")

    product_category_pairs_df = get_product_category_pairs(products_df, categories_df, prod_cat_df)
    products_without_category_df = get_products_without_categories(products_df, prod_cat_df)

    print("Pairs 'Product - Category':")
    product_category_pairs_df.show(truncate=False)

    print("Products without category:")
    products_without_category_df.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()