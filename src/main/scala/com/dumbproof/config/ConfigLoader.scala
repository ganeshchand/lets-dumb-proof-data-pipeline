package com.dumbproof.config

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.typesafe.config.{ConfigFactory, Config => TypeSafeConfig}


object ConfigLoader {

    def loadFromDBFSFile(dbfsPath: String): TypeSafeConfig = {
      require(dbfsPath.startsWith("dbfs:/"), s"Invalid dbfs path $dbfsPath. Path must start with dbfs:/")
      loadFromString(dbutils.fs.head(dbfsPath))
    }

    def loadFromFile(path: String): TypeSafeConfig = {
      val configStr = scala.io.Source.fromFile(path).getLines().mkString("\n")
      loadFromString(configStr)
    }

    def loadFromResource(resourceName: String): TypeSafeConfig = {
      ConfigFactory.load(this.getClass.getClassLoader, resourceName).resolve()
    }

    def loadFromString(confStr: String): TypeSafeConfig = {
      ConfigFactory.parseString(confStr).resolve()
    }

    def loadWith(f: => TypeSafeConfig): TypeSafeConfig = f
}
