package model

case class DataFromDB(
                       id_data: Int,
                       date: String,
                       subscriberCount: String,
                       subscribersDif: String,
                       videoCount: String,
                       videoCountDif: String,
                       viewCount: String,
                       viewCountDif: String)