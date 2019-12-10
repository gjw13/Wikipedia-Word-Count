import scala.util.matching.Regex
import scala.xml.XML
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

object WikiArticleWordCount {
    
    // regexes for stub redirect and disambiguation
    val stub_regex = "-stub}}".r
    val redirect_regex = "#redirect".r
    val disambiguation_regex = "disambiguation}}".r
    
    // function that determines whether text is an article, stub, redirect, or disambiguation
    def isArticle(v:String): Boolean = {
        if (stub_regex.findAllIn(v).length >0 ) {
            return false
        }
        if (redirect_regex.findAllIn(v).length >0 ) {
            return false
        }
        if (disambiguation_regex.findAllIn(v).length >0 ) {
            return false
        }
        else {
            return true
        }
    }
    
    // main function
    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("Wikipedia Article WordCount")
        val sc = new SparkContext(sparkConf)
        
        val wikiRDD = sc.textFile("WikiPerLine") // load output from Part 1

        // extract title and text
        val title_text = wikiRDD.map { I =>
            val line = XML.loadString(I)
            val title = (line \ "title").text
            val text = (line \\ "text").text
            (title, text)
        }
        
        // identify articles
        val article_text = title_text.filter(r=>isArticle(r._2.toLowerCase))

        //seperate by tab
        val tabbed_articles = article_text.map(f => (f._1 +"\t"+f._2))

        //save file with articles
        tabbed_articles.saveAsTextFile("WikiArticles")
        
        // print page count
        val page_count = wikiRDD.count
        println("Page count: " + page_count)

        //print article count
        val article_count = article_text.count
        println("Article count: " + article_count)

        // get total word count and sort by value
        val counts = tabbed_articles.flatMap(line => line.split(" ")).map(word=>(word.toLowerCase,1))
        val total_counts = counts.reduceByKey(_+_)
        val sorted_counts = total_counts.sortBy(_._2,false)

        // Total number of words in Wikipedia articles
        val word_count = counts.count
        println("Word count: " + word_count)

        // Total number of unique words in Wikipedia articles
        val unique_word_count = total_counts.count
        println("Unique word count: " + unique_word_count)

        // separate by tab 
        val final_output = sorted_counts.map(f => (f._1 +"\t"+ f._2))

        // save file
        final_output.saveAsTextFile("wcOutput")
        
        // stop spark context
        sc.stop()
    }
}
