from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, month, to_date, desc, row_number
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AnalyseDesVentes") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Load ventes.csv file
df = spark.read.csv("/app/ventes.csv", header=True, inferSchema=True)
df.show(5)  # Display data preview

# Check schema and row count
df.printSchema()
row_count = df.count()
print(f"Nombre total de lignes : {row_count}")

# Filter transactions with amount > 100
df_filtered = df.filter(col("amount") > 100)
df_filtered.show(5)

# Fill nulls in amount and category
df_filled = df.na.fill({"amount": 0, "category": "Inconnu"}) #df1.fillna()..
df_filled.show(5)

# Convert 'date' column to date format
df_filled = df_filled.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
df_filled.show(5)

# Total sales for the period
total_ventes = df_filled.agg(sum("amount").alias("total_ventes")).collect()[0]["total_ventes"]
print(f"Montant total des ventes : {total_ventes}")

# Total sales by category
ventes_par_categorie = df_filled.groupBy("category").agg(sum("amount").alias("total_ventes")).orderBy(desc("total_ventes"))
ventes_par_categorie.show()

# Total sales by month
ventes_par_mois = df_filled.withColumn("mois", month("date")).groupBy("mois").agg(sum("amount").alias("total_ventes")).orderBy("mois")
ventes_par_mois.show()

# Top 5 products by sales amount
top_5_produits = df_filled.groupBy("product_id").agg(sum("amount").alias("total_ventes")).orderBy(desc("total_ventes")).limit(5)
top_5_produits.show()

# Best-selling product for each category
window = Window.partitionBy("category").orderBy(desc("total_ventes"))
top_produit_par_categorie = df_filled.groupBy("category", "product_id").agg(sum("amount").alias("total_ventes"))
top_produit_par_categorie = top_produit_par_categorie.withColumn("rang", row_number().over(window)).filter(col("rang") == 1).drop("rang")
top_produit_par_categorie.show()

# Stop the Spark session
spark.stop()
