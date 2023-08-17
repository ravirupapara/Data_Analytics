def load_product_pyspark(input_file):

    spark=SparkSession.builder.appName("LoadProduct").getOrCreate()

    columns=StructType([StructField("SKU_INP",IntegerType(),True)\
                    ,StructField("SKU_DESC",StringType(),True)\
                    ,StructField("UPC_ID",StringType(),True)\
                    ,StructField("DEPARTMENT_ID",StringType(),True)\
                    ,StructField("DEPARTMENT_DESC",StringType(),True)\
                    ,StructField("CLASS_ID",StringType(),True)\
                    ,StructField("CLASS_DESC",StringType(),True)\
                    ,StructField("SUB_CLASS_ID",StringType(),True)\
                    ,StructField("BUYER_ID",StringType(),True)\
                    ,StructField("BUYER_DESC",StringType(),True)\
                    ,StructField("MANUF_ID",StringType(),True)\
                    ,StructField("MANUF_DESC",StringType(),True)\
                    ,StructField("SKU_BRAND_ID",StringType(),True)\
                    ,StructField("SKU_BRAND_DESC",StringType(),True)\
                    ,StructField("WET_DRY_CODE",StringType(),True)\
                    ,StructField("WEIGHT",StringType(),True)\
                    ,StructField("BRAND_PRODUCT_TYPE",StringType(),True)])

    product=spark.read.format("csv")\
    .options(delimiter='|',header=True)\
    .options("encoding","latin1")\
    .options(delimiter='|',header=True)\
    .load(output_path)

    product=product.withcolumn("UPC_ID",when(col("UPC_ID")==".",None).otherwise(col("UPC_ID")))

    product=product.withColumn("SKU_DESC",rtrim(product.SKU_DESC))
    product=product.withColumn("DEPARTMENT_DESC",rtrim(product.DEPARTMENT_DESC))
    product=product.withColumn("CLASS_DESC",rtrim(product.CLASS_DESC))
    product=product.withColumn("BUYER_DESC",rtrim(product.BUYER_DESC))
    product=product.withColumn("MANUF_ID",rtrim(product.MANUF_ID))
    product=product.withColumn("SKU_BRAND_DESC",rtrim(product.SKU_BRAND_DESC))


    product=product.fillna(value=0,subset=['MANUF_ID'])

    product=product.withColumn('SKU_KEY',format_string("%07d",col('SKU_INP').cast('int')))

    product.createOrReplaceTempView("product")    

    product_SKU=spark.sql("select count(distinct SKU_DESC) as distinct_vals,min(length(SKU_DESC)) as minlen,max(length(SKU_DESC)) as maxlen,min(SKU_DESC) as minval,max(SKU_DESC) as maxval from product ")
    product_SKU=product_SKU.filter(product.distinct_vals.isNotNull())
    product_SKU=product_SKU.sort(['distinct_vals'])


    product_SKU_pandas=product_SKU.toPandas()
    product_pandas=product.toPandas()


    return product_SKU_pandas,product_pandas


