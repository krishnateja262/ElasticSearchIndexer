import com.surftools.BeanstalkClientImpl.ClientImpl

/**
 * Created by krishna on 9/26/15.
 */

object TubeListenFunctions {

  def apply(port:Int, host:String)= new TubeListenFunctions(port,host)
}
class TubeListenFunctions(port:Int,host:String){

  val clientImpl = new ClientImpl(host,port)

  def getTubeStatus(tubeName:String)={
    val stats = clientImpl.statsTube(tubeName)
    stats.get("current-jobs-ready").toInt
  }

  def getTubeMessages(tubeName:String,count:Int): List[String] ={
    clientImpl.watch(tubeName)
    val noOfJobsReady = getTubeStatus(tubeName)
    val netCount = if(noOfJobsReady >= count) count else noOfJobsReady
    for(i <- List.range(0,netCount)) yield {
      val job = clientImpl.reserve(null)
      clientImpl.delete(job.getJobId)
      val k = new String(job.getData)
      k.replaceAll("\\\\","")
    }
  }
}
