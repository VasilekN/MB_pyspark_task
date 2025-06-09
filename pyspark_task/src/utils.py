
def get_product_category_pairs(products_df, categories_df, prod_cat_df):
    df = prod_cat_df.join(products_df, on="product_id", how="inner") \
                    .join(categories_df, on="category_id", how="inner")
    return df.select("product_name", "category_name")

def get_products_without_categories(products_df, prod_cat_df):
    df = products_df.join(prod_cat_df, on="product_id", how="left_anti")
    return df.select("product_id", "product_name")