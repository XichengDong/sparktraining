package org.training.spark.reco.webservice

import javax.xml.bind.annotation.XmlRootElement

@XmlRootElement
class RecommendedItems {
  private var items: Array[Long] = null

  def getItems: Array[Long] = {
    return items
  }

  def setItems(items: Array[Long]) {
    this.items = items
  }
}
