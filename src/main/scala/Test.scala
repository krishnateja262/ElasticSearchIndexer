import java.util.logging.{Level, Logger}

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri, UpdateDefinition}
import org.elasticsearch.common.settings.ImmutableSettings
import org.joda.time.{DateTime, DateTimeZone}
import org.json.{JSONArray, JSONObject}

import scala.util.Try

/**
 * Created by krishna on 6/27/15.
 */
object Test extends App{

  val logger = Logger.getLogger(this.getClass.getName)

  val dateTime = new DateTime(DateTimeZone.forID("Asia/Kolkata"))

  val settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build()

  val client = ElasticClient.remote(settings, ElasticsearchClientUri("elasticsearch://:9300"))
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
      println(exec.get.buildFailureMessage())
      logger.log(Level.SEVERE,exec.failed.get.getMessage)
    }
  }

  def tubeStringToBulkOperation(tubeName:String)={
    val objs = if(tubeListenFunctions.getTubeStatus(tubeName) > 0) tubeListenFunctions.getTubeMessages(tubeName,2) else List()

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
    if(jSONObject.has("name") && jSONObject.has("id") && jSONObject.has("productUrl") && jSONObject.has("img")
      && jSONObject.has("isProduct") && jSONObject.getBoolean("isProduct")) {
      Map(
        "images" -> jsonStringArrayToList(Try(jSONObject.getJSONArray("images")).getOrElse(new JSONArray())),
        "imgUrl" -> jSONObject.getString("img"),
        "description" -> Try(jSONObject.getString("description")).getOrElse(""),
        "discount" -> jSONObject.getDouble("discount"),
        "specifications" -> Try(jSONObject.getJSONObject("specifications")).getOrElse(new JSONObject()),
        "offerModel" -> Try(jSONObject.getJSONObject("offerModel")).getOrElse(new JSONObject()),
        "colors" -> jsonStringArrayToList(Try(jSONObject.getJSONArray("colors")).getOrElse(new JSONArray())),
        "features" -> jsonStringArrayToList(Try(jSONObject.getJSONArray("features")).getOrElse(new JSONArray())),
        "sizes" -> jsonStringArrayToList(Try(jSONObject.getJSONArray("sizes")).getOrElse(new JSONArray())),
        "sellingPrice" -> jSONObject.getInt("discountedPrice"),
        "originalPrice" -> jSONObject.getInt("price"),
        "merchant" -> jSONObject.getString("vendor"),
        "otherFeatures" -> Try(jSONObject.getJSONObject("otherFeatures")).getOrElse(new JSONObject()),
        "title" -> jSONObject.getString("name"),
        "pid" -> jSONObject.getString("id"),
        "productUrl" -> jSONObject.getString("productUrl"),
        "fashion" -> jSONObject.getBoolean("fashion"),
        "updateAt" -> dateTime.toDateTime.toString,
        "inStock" -> jSONObject.getBoolean("inStock"),
        "dates" -> jsonStringArrayToList(Try(jSONObject.getJSONArray("dates")).getOrElse(new JSONArray())),
        "rating" -> Try(jSONObject.getDouble("rating")).getOrElse(0.0),
        "isProduct" -> true
      )
    }else if(jSONObject.has("id") && jSONObject.has("isProduct") && !jSONObject.getBoolean("isProduct")){
      logger.log(Level.WARNING,jSONObject.toString())
      Map(
        "pid" -> jSONObject.getString("id"),
        "merchant" -> jSONObject.getString("vendor"),
        "isProduct" -> false
      )
    }else{
      logger.log(Level.SEVERE,jSONObject.toString())
      Map[String,AnyRef]()
    }
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
