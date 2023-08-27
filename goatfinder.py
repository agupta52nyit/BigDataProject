from pyspark.sql import SparkSession

def positions_gained(spark, input_csv, output_csv):
    # Read the CSV file using Spark
    data = spark.read.option("header", "true").csv(input_csv)
    
    # Convert columns to numeric values
    data = data.withColumn("grid", data["grid"].cast("int"))
    data = data.withColumn("position", data["position"].cast("int"))

    # Calculate positions gained for each driver
    data = data.withColumn("positions_gained", data["grid"] - data["position"])
    positions_gained_by_driver = data.groupBy('driverId').sum('positions_gained')

    # Save position gain data 
    positions_gained_by_driver.write.csv(output_csv, header=True)

def goatfinder(spark, positions_csv, drivers_csv):
    # Read Data using Spark
    positions_data = spark.read.option("header", "true").csv(positions_csv)
    drivers_data = spark.read.option("header", "true").csv(drivers_csv)

    # Merge positions gained and drivers names using driverId
    merged_data = positions_data.join(drivers_data, on='driverId')

    # Convert first and last names to uppercase
    merged_data = merged_data.withColumn("forename", merged_data["forename"].upper())
    merged_data = merged_data.withColumn("surname", merged_data["surname"].upper())

    # Save merged data
    merged_data.write.csv('goat.csv', header=True)

    # Find the driver with the most wins
    most_wins_driver = merged_data.orderBy("TotalPositionsGained", ascending=False).first()

    # Generate "goat" file 
    with open('goat.txt', 'w') as txt_file:
        txt_file.write(f"    ___  \n   [-+-]   \n []=/|\\=[]  \n   /:|:\\ \n  | /U\\ | \n  || _ ||   {most_wins_driver['forename']} {most_wins_driver['surname']} IS THE GOAT!!!\n  '\\(@)/' \n    \\Y/  \n []==U==[] \n  ___H___   \n  `--V--`⠀   ")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("GoatFinder").getOrCreate()
    
    positions_gained(spark, 's3://goatf1/results.csv', 's3://goatf1/positions_gained')
    goatfinder(spark, 's3://goatf1/positions_gained/', 's3://goatf1/drivers.csv')

    spark.stop()
