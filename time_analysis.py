# How often does each Event (Funktion) occur for each VorgangsID on average

df_grouped = df.groupBy("VorgangsID", "Funktion").count()

df_renamed = df_grouped.withColumnRenamed("count", "event_count")

df_avg = df_renamed.groupBy("Funktion").agg(
    F.avg("event_count").alias("avg_event_count")
)

df_ordered = df_avg.orderBy(F.desc("avg_event_count"))

df_ordered.show()


def calculate_throughput_time(df: DataFrame, event: str) -> DataFrame:
    # Define window specification
    window_spec = Window.partitionBy("VorgangsID").orderBy("Datum")

    # Filter DataFrame based on event
    df_filtered = df.filter(F.col("Funktion") == event)

    # Add min_date and max_date columns
    df_filtered = (
        df_filtered.withColumn("min_date", F.min("Datum").over(window_spec))
        .withColumn("max_date", F.max("Datum").over(window_spec))
        .select("VorgangsID", "Datum", "Funktion", "min_date", "max_date")
    )

    # Calculate time difference in seconds
    df_filtered = df_filtered.withColumn(
        "time_diff", F.unix_timestamp("max_date") - F.unix_timestamp("min_date")
    )

    # Convert time difference to minutes, hours, and days
    df_filtered = df_filtered.withColumn("time_diff_minutes", F.col("time_diff") / 60)
    df_filtered = df_filtered.withColumn("time_diff_hours", F.col("time_diff") / 3600)
    df_filtered = df_filtered.withColumn(
        "time_diff_days", F.col("time_diff") / (3600 * 24)
    )

    return df_filtered


# Get distinct values from the "Funktion" column
distinct_events = df.select("Funktion").distinct().rdd.flatMap(lambda x: x).collect()

# Create a dropdown widget with the distinct values
dbutils.widgets.dropdown("event", distinct_events[0], distinct_events)

# Get the selected event from the widget
selected_event = dbutils.widgets.get("event")

# Use the selected event in the function
df_filtered = calculate_throughput_time(df, selected_event)

# Show the average time difference
df_filtered.groupBy().avg("time_diff").show()


from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F


def calculate_time_diff(df: DataFrame, event1: str, event2: str) -> DataFrame:
    # Define window specification
    window_spec = Window.partitionBy("VorgangsID").orderBy("Datum")

    # Filter DataFrame based on event1 and event2
    df_filtered = df.filter(F.col("Funktion").isin([event1, event2]))

    # Calculate the time difference between the first occurrence of event1 and the first occurrence of event2
    df_filtered = (
        df_filtered.withColumn(
            "min_date_event1",
            F.min(F.when(F.col("Funktion") == event1, F.col("Datum"))).over(
                window_spec
            ),
        )
        .withColumn(
            "min_date_event2",
            F.min(F.when(F.col("Funktion") == event2, F.col("Datum"))).over(
                window_spec
            ),
        )
        .select("VorgangsID", "Datum", "Funktion", "min_date_event1", "min_date_event2")
    )

    df_filtered = df_filtered.withColumn(
        "time_diff",
        F.unix_timestamp("min_date_event2") - F.unix_timestamp("min_date_event1"),
    )

    # Convert time difference to minutes, hours, and days
    df_filtered = df_filtered.withColumn("time_diff_minutes", F.col("time_diff") / 60)
    df_filtered = df_filtered.withColumn("time_diff_hours", F.col("time_diff") / 3600)
    df_filtered = df_filtered.withColumn(
        "time_diff_days", F.col("time_diff") / (3600 * 24)
    )

    return df_filtered


# Get distinct values from the "Funktion" column
distinct_events = df.select("Funktion").distinct().rdd.flatMap(lambda x: x).collect()

# Create dropdown widgets for event1 and event2
dbutils.widgets.dropdown("event1", distinct_events[0], distinct_events)
dbutils.widgets.dropdown("event2", distinct_events[0], distinct_events)

# Get the selected events from the widgets
selected_event1 = dbutils.widgets.get("event1")
selected_event2 = dbutils.widgets.get("event2")

# Use the selected events in the function
df_filtered = calculate_time_diff(df, selected_event1, selected_event2)

# Show the average time difference
df_filtered.groupBy().avg("time_diff").show()
