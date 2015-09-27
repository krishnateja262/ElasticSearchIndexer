import java.util.logging.{Level, Logger}

import com.sksamuel.elastic4s.{UpdateDefinition, ElasticClient}
import org.elasticsearch.common.settings.ImmutableSettings
import org.joda.time.{DateTime, DateTimeZone}
import org.json.{JSONArray, JSONObject}
import com.sksamuel.elastic4s.ElasticDsl._

import scala.util.Try

/**
 * Created by krishna on 6/27/15.
 */
object Test extends App{

  val logger = Logger.getLogger(this.getClass.getName)

  val dateTime = new DateTime(DateTimeZone.forID("Asia/Kolkata"))

  val settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build()

  val client = ElasticClient.remote(settings,("",9300))
  val tubeListenFunctions = TubeListenFunctions(14711,"")

  val tubeName = "productDetailsTube"

  while(true){
    if(tubeListenFunctions.getTubeStatus(tubeName) > 0){
      val updates = tubeStringToBulkOperation(tubeName)
      if(updates.length > 0) {
        executeElasticBulkUpdate(updates)
      }
    }
    Thread.sleep(10000)
  }

  def executeElasticBulkUpdate(updates:List[UpdateDefinition])={
    val exec = Try(client.execute {
      bulk(
      updates
      )
    }.await)
    if(exec.isFailure){
      logger.log(Level.SEVERE,exec.failed.get.getMessage)
    }
  }

  def tubeStringToBulkOperation(tubeName:String)={
    val objs = if(tubeListenFunctions.getTubeStatus(tubeName) > 0) tubeListenFunctions.getTubeMessages(tubeName,100) else List()

    val generatedJsonObjects = objs.map(obj => {
      val jsonObj = Try(new JSONObject(obj))
      if(jsonObj.isFailure) Try(new JSONObject(obj.substring(1,obj.length-1))).getOrElse(new JSONObject()) else jsonObj.get
    }).filter(_.keySet().size()>0)

    val generatedMapObjects = generateMaps(generatedJsonObjects)

    for(i <- List.range(0,generatedMapObjects.length)) yield {
      val productId = generatedMapObjects(i)("merchant")+"_"+generatedMapObjects(i)("pid")
      logger.log(Level.INFO,productId)
      update id(productId) in "products/product" docAsUpsert(generatedMapObjects(i))
    }
  }

  def generateMaps(jSONObjects: List[JSONObject])={
    jSONObjects.map(jSONObject => generateMap(jSONObject)).filter(map => map.keySet.size > 0)
  }

  def generateMap(jSONObject: JSONObject)={
    Try(Map(
      "images"->jsonStringArrayToList(jSONObject.getJSONArray("images")),
      "imgUrl"-> jSONObject.getString("img"),
      "rating"-> jSONObject.getDouble("rating"),
      "description"->jSONObject.getString("description"),
      "discount"->jSONObject.getDouble("discount"),
      "specifications"->jSONObject.getJSONObject("specifications"),
      "offerModel"-> jSONObject.getJSONObject("offerModel"),
      "colors"->jsonStringArrayToList(jSONObject.getJSONArray("colors")),
      "features"->jsonStringArrayToList(jSONObject.getJSONArray("features")),
      "sizes"->jsonStringArrayToList(jSONObject.getJSONArray("sizes")),
      "sellingPrice"->jSONObject.getInt("discountedPrice"),
      "originalPrice"->jSONObject.getInt("price"),
      "merchant"->jSONObject.getString("vendor"),
      "otherFeatures"->jSONObject.getJSONObject("otherFeatures"),
      "title"->jSONObject.getString("name"),
      "pid"->jSONObject.getString("id"),
      "productUrl"->jSONObject.getString("productUrl"),
      "fashion"->jSONObject.getBoolean("fashion"),
      "updateAt"-> dateTime.toDateTime.toString
    )).getOrElse(Map())
  }

  def jsonStringArrayToList(array:JSONArray)={
    for(i <- List.range(0,array.length())) yield array.getString(i)
  }

  def otherFeaturesToMap(jSONObject: JSONObject)={
    val gender = if(jSONObject.has("gender")) jSONObject.getString("gender") else ""
    val colors = if(jSONObject.has("colors")) jsonStringArrayToList(jSONObject.getJSONArray("colors")) else List()
    Map("gender"->gender,"colors"->colors)
  }

  def offerModelToMap(jSONObject: JSONObject)={
    val effectivePrice = if(jSONObject.has("effective_price")) jSONObject.getInt("effective_price") else 0
    val offerType = if(jSONObject.has("type")) jSONObject.getString("type") else ""
    val value = if(jSONObject.has("value")) jSONObject.getInt("value") else ""
    Map("effective_price"->effectivePrice,"type"->offerType,"value"->value)
  }
}
