# import the required libraries:
from pyspark.sql import functions as F
from pyspark.sql.window import Window as W
from pyspark.sql.functions import row_number, col


def compare_rows(df_old, df_new):
    """
    Compare the number of rows between two Spark DataFrames.

    Parameters:
    - df_old (DataFrame): The original DataFrame.
    - df_new (DataFrame): The new DataFrame.

    Returns:
    None

    This function compares the number of rows between two Spark DataFrames.
    It prints a message indicating whether there are updates, deletions, or no changes.

    Example:
    >>> df1 = spark.createDataFrame([(1, 'A'), (2, 'B'), (3, 'C')], ['ID', 'Value'])
    >>> df2 = spark.createDataFrame([(1, 'A'), (2, 'B'), (4, 'D')], ['ID', 'Value'])
    >>> compare_rows(df1, df2)
    Updated: 1 row
    """
    rows_old = df_old.count()
    rows_new = df_new.count()
    if rows_old == rows_new:
        print("No Update")
    elif rows_old > rows_new:
        print(f"Deleted: {rows_old - rows_new} rows")
    else:
        print(f"Updated: {rows_new - rows_old} rows")
        print(f"Total Rows: {rows_new} rows")


def compare_schema(df_old, df_new):
    """
    Compare the schema of two Spark DataFrames and display the schema changes.

    Parameters:
    - df_old (DataFrame): The original DataFrame.
    - df_new (DataFrame): The new DataFrame.

    Returns:
    None

    This function compares the schema of two Spark DataFrames and prints any changes detected.
    It checks for changes in field names, data types, and the number of fields.

    Example:
    >>> df1 = spark.createDataFrame([(1, 'A'), (2, 'B'), (3, 'C')], ['ID', 'Value'])
    >>> df2 = spark.createDataFrame([(1, 'A'), (2, 'B'), (4, 'D')], ['ID', 'Value', 'NewColumn'])
    >>> get_schema_change(df1, df2)
    Number of Fields Change: 2 -> 3
    """

    schema_old = df_old.schema
    schema_new = df_new.schema

    if schema_old == schema_new:
        print("No Schema Change")
    else:

        if len(schema_old) == len(schema_new):

            for i in range(len(schema_old)):
                if schema_old[i].name != schema_new[i].name:
                    print(
                        f"Field Name Change: {schema_old[i].name} -> {schema_new[i].name}"
                    )
                if schema_old[i].dataType != schema_new[i].dataType:
                    print(
                        f"Data Type Change: {schema_old[i].dataType} -> {schema_new[i].dataType}"
                    )
        else:
            print(f"Number of Fields Change: {len(schema_old)} -> {len(schema_new)}")


def get_data_changes(df_old, df_new, key_column):
    """
    Compares two DataFrames based on a specified key column.

    Parameters:
    - df_old (DataFrame): The old DataFrame to compare.
    - df_new (DataFrame): The new DataFrame to compare.
    - key_column (str): The column name to use as the key for comparison.

    Returns:
    - DataFrame: A DataFrame containing rows that are different between the two input DataFrames but exist in either one.


    Example:
    >>> df1 = spark.createDataFrame([(1, 'A'), (2, 'B'), (3, 'C')], ['ID', 'Value'])
    >>> df2 = spark.createDataFrame([(1, 'A'), (2, 'D'), (4, 'E')], ['ID', 'Value'])
    >>> compare_dataframes(df1, df2, 'ID').show()
    +---+-----+
    | ID|Value|
    +---+-----+
    |  2|    D|
    |  4|    E|
    +---+-----+

    """

    intersection = df_old.select(key_column).intersect(df_new.select(key_column))

    df_old_filtered = df_old.join(intersection, key_column, "inner")
    df_new_filtered = df_new.join(intersection, key_column, "inner")

    df_old_filtered.cache()
    df_new_filtered.cache()

    df_union = df_old_filtered.unionByName(df_new_filtered, allowMissingColumns=True)

    diff_df = df_union.subtract(df_old_filtered).union(
        df_union.subtract(df_new_filtered)
    )

    return diff_df


def get_first_occurrence(df, key_column, date_column):
    """
    Display the first occurrence date for each distinct value in the specified columns.

    Parameters:
    - df (DataFrame): The input DataFrame.
    - key_column (str): The column name containing distinct values.
    - date_column (str): The column name containing dates.

    Returns:
    None

    This function takes a DataFrame as input and displays each distinct value of the key column
    along with the date of its first occurrence in the date column.

    Example:
    >>> df = spark.createDataFrame([(1, '2024-04-27'), (2, '2024-04-28'), (1, '2024-04-29')], ['ID', 'Date'])
    >>> get_first_occurrence(df, 'ID', 'Date')
    +---+----------+
    | ID|first_date|
    +---+----------+
    |  1|2024-04-27|
    |  2|2024-04-28|
    +---+----------+
    """

    window_spec = W.partitionBy(key_column).orderBy(date_column)
    first_occurrence = row_number().over(window_spec)

    df = df.withColumn("rn", first_occurrence)

    return df.filter(col("rn") == 1).select(key_column, date_column)
