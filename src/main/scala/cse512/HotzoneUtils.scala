package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    val rectSplit = queryRectangle.split(",")
    val rectLeftX = rectSplit(0).toDouble
    val rectBotY = rectSplit(1).toDouble
    val rectRightX = rectSplit(2).toDouble
    val rectTopY = rectSplit(3).toDouble

    val pointSplit = pointString.split(",")
    val pointX = pointSplit(0).toDouble
    val pointY = pointSplit(1).toDouble

    return pointX >= rectLeftX && pointX <= rectRightX && pointY >= rectBotY && pointY <= rectTopY
  }

  // YOU NEED TO CHANGE THIS PART

}
