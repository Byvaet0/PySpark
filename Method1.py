from pyspark.sql import DataFrame

def get_product_category_pairs(
    products_df: DataFrame,
    categories_df: DataFrame,
    product_category_df: DataFrame
) -> DataFrame:
    return (
        products_df
        .join(product_category_df, on="product_id", how="left")
        .join(categories_df, on="category_id", how="left")
        .select("product_name", "category_name")
    )


def get_products_without_categories(
    products_df: DataFrame,
    product_category_df: DataFrame
) -> DataFrame:
    return (
        products_df
        .join(product_category_df, on="product_id", how="left_anti")
        .select("product_name")
    )
