package config

import com.typesafe.config.{Config, ConfigFactory}

object ConfigHelper {
  //加载配置文件
  private lazy val load: Config = ConfigFactory.load()
  //加载jdbc相关的
  val url: String = load.getString("url")
  val driver: String = load.getString("driver")
  val userName: String = load.getString("userName")
  val password: String = load.getString("password")
}
