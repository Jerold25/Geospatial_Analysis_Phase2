package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo.createOrReplaceTempView("pickupinfotemp")

  // clip pickupInfo to only include necessary coordinates
  val pickupInfoClipped = spark.sql(s"select * from pickupinfotemp where x>=$minX and x<=$maxX and y>=$minY and y<=$maxY and z>=$minZ and z<=$maxZ")
  pickupInfoClipped.createOrReplaceTempView("pickupinfo")

  // calculate all xj values
  val numCellPickups = spark.sql("select x, y, z, count(x, y, z) as numpickups from pickupinfo group by x, y, z")
  numCellPickups.createOrReplaceTempView("numcellpickups")

  // calculate Xbar = sum(j=1->numCells, xj) / numCells
  val numCellPickupsAvg = spark.sql(s"select sum(numpickups)/$numCells as value from numcellpickups")
  numCellPickupsAvg.createOrReplaceTempView("numcellpickupsavg")

  // calculate S = sqrt((sum(j=1->numCells, xj^2) / numCells) - Xbar^2)
  val numCellPickupsSquareAvg = spark.sql(s"select sum(power(numcellpickups.numpickups, 2))/$numCells as value from numcellpickups")
  numCellPickupsSquareAvg.createOrReplaceTempView("numcellpickupssquareavg")
  val s = spark.sql("select sqrt(numcellpickupssquareavg.value-power(numcellpickupsavg.value, 2)) as value from numcellpickupsavg cross join numcellpickupssquareavg")
  s.createOrReplaceTempView("s")

  // get wij: number of neighbors of i and total count of pickups for all neighbors of i
  val neighborNumCellPickups = spark.sql("select ncp1.x as x, ncp1.y as y, ncp1.z as z, coalesce(sum(ncp2.numpickups), 0) as value, coalesce(count(ncp2.x, ncp2.y, ncp2.z), 0) as numneighs from numcellpickups ncp1 left join numcellpickups ncp2 on abs(ncp1.x-ncp2.x)<=1 and abs(ncp1.y-ncp2.y)<=1 and abs(ncp1.z-ncp2.z)<=1 group by ncp1.x, ncp1.y, ncp1.z")
  neighborNumCellPickups.createOrReplaceTempView("neighbornumcellpickups")

  // calculate Gi* for all i=1->numCells =
  // (sum(j=1->numCells, wij*xj) - Xbar*sum(j=1->numCells, wij)) / (S * sqrt((n*sum(j=1->numCells, wij^2) - sum(j=1->numCells, wij)^2) / (n-1)))
  val g = spark.sql(s"select nncp1.x as xi, nncp1.y as yi, nncp1.z as zi, ( (nncp1.value - ncpa.value*nncp1.numneighs) / (s.value * (sqrt(($numCells*nncp2.numneighs - power(nncp2.numneighs, 2)) / ($numCells - 1)))) ) as gi from neighbornumcellpickups as nncp1 cross join numcellpickupsavg as ncpa, neighbornumcellpickups as nncp2 cross join s where nncp1.x=nncp2.x and nncp1.y=nncp2.y and nncp1.z=nncp2.z order by gi desc")
  g.createOrReplaceTempView("g")

  val result = spark.sql("select xi, yi, zi from g")

  return result // YOU NEED TO CHANGE THIS PART
}
}
