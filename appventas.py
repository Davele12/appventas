from pyspark.sql import SparkSession

# Iniciar la sesión de Spark
spark = SparkSession.builder.appName("VentasTienda").getOrCreate()

# Cargar el archivo de texto como un RDD
rdd_ventas = spark.sparkContext.textFile("/home/vagrant/labSpark/app_ventas/ventas.txt")

# Convertir cada línea del RDD en una tupla con tres elementos
rdd_tuplas = rdd_ventas.map(lambda x: tuple(x.split(",")))

# Calcular el total de ventas por producto
rdd_ventas_por_producto = rdd_tuplas.map(lambda x: (x[0], float(x[1])*int(x[2]))).reduceByKey(lambda x, y: x+y)

# Calcular el total de ventas para toda la tienda
total_ventas = rdd_ventas_por_producto.values().reduce(lambda x, y: x+y)

# Encontrar el producto más vendido
producto_mas_vendido = rdd_ventas_por_producto.sortBy(lambda x: -x[1]).first()

# Convertir el RDD en un DataFrame
df_ventas = spark.createDataFrame(rdd_tuplas, ["producto", "precio", "cantidad"])

# Utilizar SQL para realizar consultas
df_ventas.createOrReplaceTempView("ventas")
spark.sql("SELECT producto, SUM(precio*cantidad) AS ventas_por_producto FROM ventas GROUP BY producto").show()
spark.sql("SELECT SUM(precio*cantidad) AS total_ventas FROM ventas").show()
spark.sql("SELECT producto, SUM(precio*cantidad) AS ventas_por_producto FROM ventas GROUP BY producto ORDER BY ventas_por_producto DESC LIMIT 1").show()

# Cerrar la sesión de Spark
spark.stop()

