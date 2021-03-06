// Twitter Hash-tag Analysis - Count the trending hash-tags and arrange them in the descending order
package com.df.hta

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.spark.streaming.flume._
import scala.util.parsing.json._

object HashtagAnalyzer {
	def main(args: Array[String]) {
		if (args.length < 2) {
			System.err.println("Usage: Hashtag Analyzer <hostname> <port>")
			System.exit(1)
		}

		val sparkConf = new SparkConf().setAppName("Weblogs Parser")
				val ssc = new StreamingContext(sparkConf, Seconds(1))

				val rawLines = FlumeUtils.createStream(ssc, args(0), args(1).toInt)

				val result = rawLines.map{record => {
					(new String(record.event.getBody().array()))
				}
		    }
//		{"in_reply_to_status_id_str":null,"in_reply_to_status_id":null,"created_at":"Sun Aug 30 13:00:00 +0000 2015","in_reply_to_user_id_str":null,"source":"<a href=\"https://roundteam.co\" rel=\"nofollow\">RoundTeam<\/a>","retweeted_status":{"in_reply_to_status_id_str":null,"in_reply_to_status_id":null,"possibly_sensitive":false,"coordinates":null,"created_at":"Sun Aug 30 12:38:23 +0000 2015","truncated":false,"in_reply_to_user_id_str":null,"source":"<a href=\"http://www.mba-exchange.com\" rel=\"nofollow\">Mba Jobs in Pacific<\/a>","retweet_count":1,"retweeted":false,"geo":null,"filter_level":"low","in_reply_to_screen_name":null,"entities":{"urls":[{"display_url":"MBA-Exchange.com/candidates/mba\u2026","indices":[68,90],"expanded_url":"http://www.MBA-Exchange.com/candidates/mba_jobs_external.php?u=aHR0cDovL3d3dy5pbmRlZWQuY29tL3JjL2Nsaz9qaz01NTFlNjg3YjYzMzU0YmQ0JmF0az0=&jk=551e687b63354bd4","url":"http://t.co/VRxIuCqF3w"}],"hashtags":[{"indices":[25,33],"text":"manager"}],"user_mentions":[],"trends":[],"symbols":[]},"id_str":"637967598928334848","in_reply_to_user_id":null,"favorite_count":0,"id":637967598928334848,"text":"Portfolio Risk Analytics #manager Lending Club, San Francisco, CA. http://t.co/VRxIuCqF3w","place":null,"contributors":null,"lang":"da","user":{"utc_offset":7200,"friends_count":0,"profile_image_url_https":"https://pbs.twimg.com/profile_images/1860437311/twitter_mbax_bigger_bigger_normal.jpg","listed_count":78,"profile_background_image_url":"http://abs.twimg.com/images/themes/theme1/bg.png","default_profile_image":false,"favourites_count":0,"description":"The best and most relevant jobs in Pacific for MBA students and alumni: Alaska, Washington, Oregon, California, Hawaii","created_at":"Tue Feb 28 16:56:34 +0000 2012","is_translator":false,"profile_background_image_url_https":"https://abs.twimg.com/images/themes/theme1/bg.png","protected":false,"screen_name":"MBA_Jobs_USA_9","id_str":"507624741","profile_link_color":"0084B4","id":507624741,"geo_enabled":false,"profile_background_color":"C0DEED","lang":"en","profile_sidebar_border_color":"C0DEED","profile_text_color":"333333","verified":false,"profile_image_url":"http://pbs.twimg.com/profile_images/1860437311/twitter_mbax_bigger_bigger_normal.jpg","time_zone":"Bern","url":"http://www.mba-exchange.com","contributors_enabled":false,"profile_background_tile":false,"statuses_count":67278,"follow_request_sent":null,"followers_count":187,"profile_use_background_image":true,"default_profile":true,"following":null,"name":"MBA Jobs in USA","location":"Geneva, Switzerland","profile_sidebar_fill_color":"DDEEF6","notifications":null},"favorited":false},"retweet_count":0,"retweeted":false,"geo":null,"filter_level":"low","in_reply_to_screen_name":null,"id_str":"637973039725068289","in_reply_to_user_id":null,"favorite_count":0,"id":637973039725068289,"text":"RT @MBA_Jobs_USA_9: Portfolio Risk Analytics #manager: Lending Club, San Francisco, CA. http://t.co/VRxIuCqF3w","place":null,"lang":"da","favorited":false,"possibly_sensitive":false,"coordinates":null,"truncated":false,"timestamp_ms":"1440939600293","entities":{"urls":[{"display_url":"MBA-Exchange.com/candidates/mba\u2026","indices":[88,110],"expanded_url":"http://www.MBA-Exchange.com/candidates/mba_jobs_external.php?u=aHR0cDovL3d3dy5pbmRlZWQuY29tL3JjL2Nsaz9qaz01NTFlNjg3YjYzMzU0YmQ0JmF0az0=&jk=551e687b63354bd4","url":"http://t.co/VRxIuCqF3w"}],"hashtags":[{"indices":[45,53],"text":"manager"}],"user_mentions":[{"indices":[3,18],"screen_name":"MBA_Jobs_USA_9","id_str":"507624741","name":"MBA Jobs in USA","id":507624741}],"trends":[],"symbols":[]},"contributors":null,"user":{"utc_offset":-14400,"friends_count":123,"profile_image_url_https":"https://pbs.twimg.com/profile_images/2936881110/e3cc7fe45a726517abcafc582428e1f4_normal.png","listed_count":252,"profile_background_image_url":"http://abs.twimg.com/images/themes/theme7/bg.gif","default_profile_image":false,"favourites_count":0,"description":"NC State University's MBA Career Development office.","created_at":"Thu May 29 12:16:43 +0000 2008","is_translator":false,"profile_background_image_url_https":"https://abs.twimg.com/images/themes/theme7/bg.gif","protected":false,"screen_name":"NCSUMBACareer","id_str":"14943431","profile_link_color":"990000","id":14943431,"geo_enabled":false,"profile_background_color":"EBEBEB","lang":"en","profile_sidebar_border_color":"DFDFDF","profile_text_color":"333333","verified":false,"profile_image_url":"http://pbs.twimg.com/profile_images/2936881110/e3cc7fe45a726517abcafc582428e1f4_normal.png","time_zone":"Eastern Time (US & Canada)","url":"http://poole.ncsu.edu/mba/career-resources/","contributors_enabled":false,"profile_background_tile":false,"profile_banner_url":"https://pbs.twimg.com/profile_banners/14943431/1354742554","statuses_count":60528,"follow_request_sent":null,"followers_count":210,"profile_use_background_image":true,"default_profile":false,"following":null,"name":"NCSUMBACareer","location":"Raleigh, NC","profile_sidebar_fill_color":"F3F3F3","notifications":null}}
			.map{ line => {
			  val parsedJson = JSON.parseFull(line)
			  
			  parsedJson.get.asInstanceOf[Map[String, Any]]("text").asInstanceOf[String]
			}
			}
//			Portfolio Risk Analytics #manager Lending Club, San Francisco, CA. http://t.co/VRxIuCqF3w
			.flatMap { tweet => {
			  tweet.split(" ")
			}
			}
			.map{ rec => (rec, 1)}
			.filter { token => token._1.startsWith("#") }
			.reduceByKeyAndWindow((a:Int, b:Int) => a+b, Seconds(120), Seconds(30))
      .transform{rec =>{
         rec.sortBy(_._2, false)
      }
	    }

			
			result.print()
			ssc.start()
			ssc.awaitTermination()
	}
}
//bin/spark-submit --jars ../spark-streaming-flume-assembly_2.11-2.0.0.jar  --class com.df.hta.HashtagAnalyzer ../streamHtJob.jar localhost 9099
