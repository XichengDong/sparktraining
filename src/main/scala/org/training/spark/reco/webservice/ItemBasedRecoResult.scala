package org.training.spark.reco.webservice

import org.training.spark.proto.Spark.{ItemSimilarities, ItemSimilarity, ItemList}
import org.training.spark.util.RedisClient
import redis.clients.jedis.Jedis
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import java.util._
import scala.collection.JavaConversions._

@Path("/ws/v1/reco")
class ItemBasedRecoResult {
  private[webservice] var jedis: Jedis = null
  @GET
  @Path("/{userid}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getRecoItems(@PathParam("userid") userid: String): RecommendedItems = {
    val recommendedItems: RecommendedItems = new RecommendedItems

    val jedis = RedisClient.pool.getResource
    val key: String = String.format("UI:%s", userid)
    val value: String = jedis.get(key)
    if (value == null || value.length <= 0) {
      return recommendedItems
    }
    val userItems = ItemList.parseFrom(value.getBytes())
    val userItemsSet = new TreeSet(userItems.getItemIdsList)
    val userItemStrs = userItems.getItemIdsList.map("II:" + _)

    val similarItems: List[String] = jedis.mget(userItemStrs:_*)
    val similarItemsSet: Set[ItemSimilarity] = new TreeSet[ItemSimilarity]
    for (item <- similarItems) {
      val result = ItemSimilarities.parseFrom(item.getBytes())
      similarItemsSet.addAll(result.getItemSimilaritesList)
    }

    val recommendedItemIDs = similarItemsSet
        .filter(item => !userItemsSet.contains(item.getItemId))
        .take(10)
        .map(_.getItemId)

    recommendedItems.setItems(recommendedItemIDs.toArray)
    return recommendedItems
  }
}
