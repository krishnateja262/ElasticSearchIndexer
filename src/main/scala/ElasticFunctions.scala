import com.sksamuel.elastic4s.{UpdateDefinition, ElasticClient}
import com.sksamuel.elastic4s.ElasticDsl._
/**
 * Created by krishna on 6/27/15.
 */
object ElasticFunctions {

  val client = ElasticClient.remote("128.199.150.107",9300)

  def getCount(indexName:String)={
    client.execute{
      count from indexName
    }
  }

  def deleteIndex(indexName:String)={
    client.execute{
      delete index indexName
    }
  }

  def createIndexWithType(indexName:String,typeName:String,fieldValues:Map[String,String])={
    client.execute{
      index into indexName/typeName fields fieldValues
    }
  }

  def upsertDocument(idValue:String,indexName:String,typeName:String,fieldValues:Map[String,String])={
    client.execute{
      update id idValue in indexName/typeName docAsUpsert(fieldValues)
    }
  }

  def searchDocument(indexName:String,typeName:String,field:String)={
    client.execute{
      search in indexName/typeName query field
    }
  }

  def bulkUpsertDocuments(bulks:List[UpdateDefinition])={
    client.execute{
      bulk(bulks)
    }
  }

  //blocking function
  def pidToMoid(indexName:String,typeName:String,pid:String)={
    client.execute{
      search in indexName/typeName query pid
    }.await.getHits.getAt(0).getId
  }
}
