package org.test.example.common
import java.util.Properties
/**
 * @author Ankit Beohar
 * This is to read sql queries from property file.
 * */

import scala.io.Source
object PropertiesLoader {

	var properties : Properties = null
			def initialLoad(){
	val url = getClass.getResource("application.properties")
			if (url != null) {
				val source = Source.fromURL(url)

						properties = new Properties()
						properties.load(source.bufferedReader())
			}

}

}