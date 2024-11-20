from mylib.lib import (
    extract,
    load_data,
    query,
    transform,
    start_spark,
    end_spark,
)


def main():
    # Extract data
    extract()
    # Start Spark session
    spark = start_spark("nypd_shooting")
    # Load data into DataFrame
    df = load_data(spark)
    # Query
    query(
        spark,
        df,
        """SELECT *
           FROM nypd_shooting 
           WHERE incident_key = 279473159 """, "nypd_shooting",
    )
    # transform by borough cardinal direction
    transform(df)
    # end spark session
    end_spark(spark)


if __name__ == "__main__":
    main()