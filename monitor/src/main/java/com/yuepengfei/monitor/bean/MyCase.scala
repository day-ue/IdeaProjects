package com.yuepengfei.monitor.bean
/**
 * 本来是官方的一个类。可是自己一直引不了就直接把代码复制到这里了
 * @param topic
 * @param partition
 */
case class TopicAndPartition(topic: String, partition: Int) {
  def asTuple = (topic, partition)
  override def toString = "[%s,%d]".format(topic, partition)
}
