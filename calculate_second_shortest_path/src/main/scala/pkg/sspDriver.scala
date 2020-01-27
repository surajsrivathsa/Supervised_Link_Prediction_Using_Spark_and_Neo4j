/*
Authors: Pramod Bontha/ Suraj
Date: 2020/01/27
Description: Calculate second shortest path using prege;l
 */

package pkg

import org.apache.spark.graphx
import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set

object sspDriver {

  def main(args: Array[String]):Unit = {

    val const = new constantsFile();
    val spark = SparkSession.builder.master(const.master).appName(const.appName).getOrCreate();

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val fsutils = new sspUtilities(spark);

    val all_graphs = fsutils.generateSubgraphsFromFile(const);
    fsutils.joinSSPData(const)
    val trainingGraph = all_graphs(0); val testingGraph = all_graphs(1);
    println("Count of edges in training and testing graph are: " + trainingGraph.edges.count() + " | " + testingGraph.edges.count())

    var trainingDistancesList = new ListBuffer[(VertexId, VertexId, Double, Double)]()
    var testingDistancesList = new ListBuffer[(VertexId, VertexId, Double, Double)]()


    //for training, The below author pairs can be taken from an rdd, we have just harcoded for some of the samples
    //2724293929|2730921238
    var startTime = System.currentTimeMillis()
    val myvalues_training = Seq((2562298254L,2563127877L),
      (2167334206L,2591971659L),
      (2831614637L,2836306953L),
      (2830597405L,2852143850L),
      (2440131172L,2474512173L),
      (2269656395L,2440835444L),
      (339716818L,2223730170L),
      (2865738722L,2872946301L),
      (94392942L,2845676227L),
      (2348405074L,2866454972L),
      (2139088336L,2289852722L),
      (2441198756L,2659249879L),
      (2848349856L,2866375232L),
      (2465988296L,2468827415L),
      (2530062680L,2893193656L),
      (2825040835L,2850194034L),
      (2823650827L,2876832378L),
      (2472000470L,2562915689L),
      (1964707850L,2778262226L),
      (2443099215L,2496268527L),
      (2748146478L,2852716795L),
      (614940499L,2471807861L),
      (2830464519L,2861064872L),
      (1970792814L,2844687314L),
      (1839925873L,2102314836L),
      (1963520853L,2806264699L),
      (2746157214L,2868781990L),
      (2689598751L,2744383273L),
      (1284829237L,2461460531L),
      (2622550371L,2834619316L),
      (2186959458L,2396266217L),
      (2442481337L,2833818190L),
      (1925053625L,2835229613L),
      (2200983177L,2401095987L),
      (2525739720L,2535980115L),
      (2430017764L,2502529688L),
      (2502432328L,2709186367L),
      (1948802595L,2500984655L),
      (308545259L,2811495711L),
      (2833879810L,2879776032L),
      (1995242560L,2816125380L),
      (2814589505L,2839460149L),
      (2124109618L,2211791816L),
      (2684595887L,2892286347L),
      (2816382341L,2845412361L),
      (1970253295L,2889443382L),
      (1989185179L,2866027463L),
      (2490724116L,2869032306L),
      (2033233654L,2881033900L),
      (2286213894L,2849194817L),
      (1974875432L,2706031651L),
      (934151860L,935546297L),
      (2699938595L,2715368198L),
      (2551699654L,2755780269L),
      (785782360L,2053872422L),
      (2134447293L,2702329570L),
      (2661312721L,2760259487L),
      (2016730383L,2750320136L),
      (198932563L,2062875396L),
      (2114455792L,2512667867L),
      (1721257957L,2163974023L),
      (1466822755L,2656952359L),
      (341797089L,2490693087L),
      (405997756L,2570789690L),
      (1190254016L,2342922472L),
      (2030694950L,2615424630L),
      (2400122170L,2718817331L),
      (38812608L,431972396L),
      (2062871982L,2223081083L),
      (656678641L,2102795435L),
      (1867053589L,2703601950L),
      (315232196L,2658398498L),
      (1488765183L,2766720660L),
      (1985945423L,2167242429L),
      (1698423755L,2589466280L),
      (1268027001L,2126536633L),
      (2141855675L,2658301840L),
      (616751981L,2341475018L),
      (2002365455L,2669794332L),
      (2630441910L,2635931760L),
      (217281801L,2652843134L),
      (1978659444L,2672201558L),
      (2646652946L,2652843134L),
      (1910881071L,2105044904L),
      (2569972414L,2687245730L),
      (2308877729L,2662808696L),
      (2146188094L,2679208912L),
      (356567120L,2425452115L),
      (2700090918L,2719755955L),
      (2104211081L,2261209647L),
      (967929143L,977264449L),
      (2299571818L,2515202408L),
      (2167802332L,2170300618L),
      (2575397680L,2666388008L),
      (2677879393L,2858335519L),
      (2165564152L,2395484070L),
      (2299373767L,2649460678L),
      (2436210153L,2603066754L),
      (381338718L,2099237704L),
      (2235023996L,2695321650L),
      (2549349459L,2662563428L),
      (2172233121L,2222223156L),
      (2652936223L,2679161780L),
      (2142726774L,2166962825L),
      (2639952394L,2790567388L),
      (1973529786L,2018653587L),
      (2562197011L,2589252256L),
      (1929053212L,2154572461L),
      (2593500530L,2663848814L),
      (1288903981L,2124945388L),
      (1843376285L,2749631936L),
      (2049044873L,2098285258L),
      (2213440551L,2219629935L),
      (2100294221L,2103982656L),
      (55904127L,2288234787L),
      (2620618968L,2620744920L),
      (289970210L,2243791763L),
      (49618465L,2075106544L),
      (2650808079L,2672038297L),
      (2591636317L,2604043913L),
      (2748001653L,2749007211L),
      (1964899704L,2306734450L),
      (119287359L,2614556540L),
      (2021872098L,2662624960L),
      (2150192452L,2242691878L),
      (1366138794L,2281978148L),
      (2108277071L,2156585922L),
      (1974810679L,2760092914L),
      (2009925864L,2735750525L),
      (1684402426L,2048073801L),
      (2143997015L,2239897238L),
      (2022682751L,2128361341L),
      (1543166843L,2038865370L),
      (2595280903L,2752266347L),
      (1772060423L,2091920247L),
      (2131777197L,2586954982L),
      (1040590662L,2005074676L),
      (323852978L,2120514170L),
      (2150740517L,2159850298L),
      (1943471593L,2267672592L),
      (2610321457L,2611088646L),
      (2130280784L,2276923904L),
      (2604223086L,2704949630L),
      (314661503L,2212676784L),
      (2281969359L,2612665657L),
      (2112419327L,2643784881L),
      (2752169971L,2753552916L),
      (2153966587L,2422226580L),
      (1976392323L,2076644049L),
      (2147587519L,2431237860L),
      (2052087786L,2506617704L),
      (2012609047L,2651389845L),
      (2778382135L,2779123016L),
      (1902780915L,2411532415L),
      (2646584563L,2671972055L),
      (268740349L,2019720406L),
      (2743049329L,2743810551L),
      (2110199789L,2761390267L),
      (209512413L,2130803149L),
      (2596533853L,2734548203L),
      (2158629249L,2737566685L),
      (2592382773L,2594640660L),
      (938206814L,2580692723L),
      (328661261L,2169852269L),
      (2752976122L,2753728457L),
      (2012229022L,2748723017L),
      (2608793671L,2645773172L),
      (2595960816L,2642865255L),
      (2621112732L,2739730719L),
      (296971239L,2205837715L),
      (1474505369L,2750739872L),
      (1989499509L,2219530053L),
      (347174450L,2496909454L),
      (2146465264L,2157599302L),
      (2068324639L,2131052321L),
      (2764518086L,2764792798L),
      (13037463L,2758945229L),
      (2382379901L,2713916867L),
      (2073906439L,2637166888L),
      (2621258336L,2662902036L),
      (1642754630L,2600087392L),
      (2041429483L,2306099050L),
      (2120653300L,2157851617L),
      (2135808315L,2150036570L),
      (181258321L,2736546305L),
      (2029813250L,2210873447L),
      (2506797782L,2527348768L),
      (1978454326L,2235439927L),
      (879329649L,2618501275L),
      (2761740401L,2763124915L),
      (170048835L,2573008527L),
      (2075596674L,2097629655L),
      (2620149590L,2648473970L),
      (2107473140L,2563049454L),
      (693339150L,1759821797L),
      (2670645147L,2744953194L),
      (2000518422L,2514856730L),
      (2110546677L,2657075382L),
      (2434124352L,2756477447L),
      (45334556L,2598291974L));

    for(vals <- myvalues_training)
      {
        //call pregel algorithm
        var tmp = generateSecondShortestPath(trainingGraph,vals._1, vals._2);
        if(tmp._3 != Double.PositiveInfinity)
          {
            println(tmp._1 + " | " +  tmp._2 + " | " + tmp._3 + " | " + tmp._4)
          }
        trainingDistancesList += tmp

      }

    var m1 = spark.sparkContext.parallelize(trainingDistancesList)
    m1.map(line => line._1 + "|"+line._2+"|"+line._3+"|"+line._4).saveAsTextFile(const.dstFolderPath + const.trainingsspFolder)


    val stopTime = System.currentTimeMillis()
    println("time taken for training datain seconds: " + (stopTime-startTime)/1000)




    ///For testing, The below author pairs can be taken from an rdd, we have just harcoded for some of the samples

    var startTime1 = System.currentTimeMillis()

    val myvalues_testing= Seq((2091037587L,2876440542L),
      (2357152457L,2752303964L),
      (1835980201L,2317327170L),
      (1977801701L,2113950212L),
      (2737407756L,2737666563L),
      (2816645120L,2859691957L),
      (2772787141L,2845071041L),
      (2135667072L,2845393220L),
      (2461654369L,2465446325L),
      (2097458924L,2659873352L),
      (2461654369L,2473066787L),
      (2430471364L,2473265844L),
      (2800300840L,2802934564L),
      (2402920589L,2431968047L),
      (2603862528L,2632136428L),
      (2292690741L,2474717616L),
      (2721100752L,2881089072L),
      (2832445162L,2861321942L),
      (2425517648L,2566799581L),
      (2652768727L,2721626688L),
      (2888072209L,2888647510L),
      (2844532254L,2868563147L),
      (2085836895L,2306285748L),
      (2375086154L,2878524190L),
      (2577225055L,2577813353L),
      (2523522009L,2833450340L),
      (349916339L,1966976961L),
      (2751651383L,2843066996L),
      (2350553418L,2381476761L),
      (2099208368L,2138454048L),
      (2637353251L,2855825854L),
      (2111472597L,2213072378L),
      (2497031294L,2747687590L),
      (1985958887L,2199617842L),
      (2462169594L,2471768262L),
      (1217839158L,2836009584L),
      (2383636145L,2440588375L),
      (2473339985L,2780882180L),
      (552026183L,2053019548L),
      (2072044168L,2158852573L),
      (1848118826L,2862805653L),
      (108341026L,2884827033L),
      (1978703801L,2158405780L),
      (2857354203L,2876333102L),
      (2116074389L,2284331943L),
      (20010182L,2647285195L),
      (308327270L,1878036863L),
      (189889569L,1248853694L),
      (2549454402L,2866016605L),
      (2822626454L,2853252515L),
      (2641432931L,2791818090L),
      (2831992160L,2838452873L),
      (307521022L,2870469381L),
      (2814913606L,2823199484L),
      (284152674L,2757672817L),
      (2463773605L,2464795414L),
      (333542820L,2857100364L),
      (1934383264L,1995546564L),
      (1204738455L,2803457535L),
      (1991069660L,2343760743L),
      (2116411267L,2299896650L),
      (2526076170L,2526180017L),
      (2579057606L,2867455080L),
      (2511602501L,2649187500L),
      (1930433806L,2117825652L),
      (2106521272L,2444096354L),
      (2421325248L,2687080627L),
      (2290761856L,2471098938L),
      (2828189048L,2829951353L),
      (2106559083L,2158040332L),
      (1788283680L,2779511621L),
      (1298010557L,2555269787L),
      (2845390881L,2879433275L),
      (2824336778L,2881405410L),
      (2830173498L,2877485516L),
      (2162535486L,2804899020L),
      (2529286260L,2701369169L),
      (1894189262L,2745062130L),
      (1968740560L,2659203386L),
      (1974256522L,2193336985L),
      (2835681078L,2851419716L),
      (1973623087L,2633936784L),
      (2143297035L,2518545728L),
      (2204836212L,2589363345L),
      (669432795L,2081153979L),
      (2105666228L,2139606776L),
      (2298638007L,2772116157L),
      (2574115253L,2841644690L),
      (2761172522L,2771309370L),
      (1961553090L,2482116528L),
      (2429444919L,2437362129L),
      (2089685747L,2120026399L),
      (2832862235L,2878571999L),
      (2245744261L,2245867665L),
      (2020225340L,2683789891L),
      (1829142945L,2081492195L),
      (2837479027L,2846295069L),
      (181990813L,1287023231L),
      (2463988067L,2473181607L),
      (2292970800L,2421871624L),
      (1997060520L,2444205398L),
      (1967384577L,2583817211L),
      (2116256453L,2675523790L),
      (49666854L,925267300L),
      (2098706453L,2167384094L),
      (2621604440L,2703943371L),
      (2033664812L,2133633675L),
      (697303759L,2629167568L),
      (392960480L,2192921912L),
      (992409725L,1958536144L),
      (2241057259L,2553574884L),
      (2396791477L,2705047697L),
      (251925956L,2442839225L),
      (2432885148L,2438622330L),
      (1939492802L,2029668504L),
      (301768398L,2130913351L),
      (2772451355L,2773907685L),
      (2429765231L,2439892785L),
      (2628172778L,2683070643L),
      (57820058L,2085083845L),
      (2428798779L,2652710315L),
      (2124644017L,2296485618L),
      (2433866391L,2440519718L),
      (2108332778L,2133792951L),
      (2311543836L,2336009019L),
      (2676144765L,2706408612L),
      (297589394L,2170511104L),
      (1554722064L,2321354278L),
      (1993858434L,2308126047L),
      (2574139334L,2609104889L),
      (266825964L,2097063636L),
      (2124522029L,2149183285L),
      (2596269021L,2596313277L),
      (757836772L,2653481604L),
      (2605064372L,2626979693L),
      (1982697301L,2001398374L),
      (2201402841L,2426790553L),
      (2737208443L,2737597264L),
      (2114855417L,2312833596L),
      (2279004066L,2287626693L),
      (2074498890L,2598661284L),
      (2311965351L,2595813021L),
      (2132180297L,2730010214L),
      (2566454461L,2696118599L),
      (2212494986L,2401336603L),
      (2562348814L,2632612791L),
      (2109769150L,2225505962L),
      (2340844920L,2742135052L),
      (2338498210L,2597512879L),
      (2736050038L,2756878406L),
      (2406519693L,2597139648L),
      (326360204L,2128018952L),
      (2641193064L,2744637462L),
      (2766202481L,2766354066L),
      (2086395809L,2123581512L),
      (2398989237L,2403228837L),
      (1600042908L,1929759189L),
      (2127520590L,2482914359L),
      (2052848173L,2133936721L),
      (1911902191L,2759757619L),
      (2118181370L,2696581299L),
      (2118248766L,2156135925L),
      (1159725865L,1215359179L),
      (573776099L,2657918698L),
      (439014917L,1972027636L),
      (2480091923L,2773106949L),
      (167846839L,2649402773L),
      (1154091960L,1976712321L),
      (2531155897L,2666311649L),
      (2242818534L,2409416060L),
      (656999898L,2107623673L),
      (2278697151L,2341660228L),
      (1962654048L,2307297651L),
      (214329078L,2686565427L),
      (2633874551L,2714729660L),
      (2617800046L,2677314515L),
      (2480488867L,2646992490L),
      (2430915914L,2605382031L),
      (12209290L,2360419373L),
      (1899994647L,2641204509L),
      (214810273L,1493979058L),
      (350116960L,2847333616L),
      (1217621955L,2765560156L),
      (288485870L,2425204061L),
      (2683116691L,2701460255L),
      (1422234431L,2703178748L),
      (2545276843L,2669457956L),
      (2593355511L,2631423405L),
      (2425922068L,2645037781L),
      (1934037773L,2067613284L),
      (154469101L,2564200368L),
      (2100722549L,2489400420L),
      (2008138857L,2689366646L),
      (223435631L,2136553196L),
      (2630415609L,2669770123L),
      (2667611634L,2763980146L),
      (2651991234L,2660525416L),
      (2000801331L,2764064144L),
      (2103779824L,2141414064L),
      (707532111L,2347186621L));


    for(vals <- myvalues_testing)
    {
      var tmp = generateSecondShortestPath(testingGraph,vals._1, vals._2);
      if(tmp._3 != Double.PositiveInfinity)
      {
        println(tmp._1 + " | " +  tmp._2 + " | " + tmp._3 + " | " + tmp._4)
      }
      testingDistancesList += tmp

    }

    var m2 = spark.sparkContext.parallelize(testingDistancesList)
    m2.map(line => line._1 + "|"+line._2+"|"+line._3+"|"+line._4).saveAsTextFile(const.dstFolderPath + const.testingsspFolder)


    val stopTime1 = System.currentTimeMillis()
    println("time taken for testing data in seconds: " + (stopTime1-startTime1)/1000)


    //val myres = generateSecondShortestPath(trainingGraph,2521364598L,2522239919L)




  }


  def generateSecondShortestPath(graph: Graph[Row, Row], srcId: graphx.VertexId, tgtId: graphx.VertexId): (VertexId, VertexId, Double, Double)= {

    val gx = graph.mapVertices( (id, _) =>
      if (id == srcId) Array(0.0,0.0, id, id)
      else Array(Double.PositiveInfinity,Double.PositiveInfinity, id, id)).mapEdges( e => 1 )

    val msssp = gx.pregel(Array(Double.PositiveInfinity,  Double.PositiveInfinity, -1, -1),7,EdgeDirection.Out)(
      (id, dist, newDist) =>
      {
        //update vertex with appropriate hop count
        //always compare the recieved message with existing count values take which ever two values that are the least and update the vertex
        if (dist(0) <= newDist(0) && dist(1) <= newDist(1))
        {
          //println(id);
          dist
        }
        else if(dist(0) <= newDist(0) && dist(1) > newDist(1))
        {
          //println(dist.mkString(" ---dist--- ")+ " --- "+ newDist.mkString(" newDist "))
          Array(dist(0), newDist(1), dist(2))
        }
        else if(dist(0) > newDist(0) && dist(1) <= newDist(1) )
        {
          Array(newDist(0), dist(1), newDist(2))
        }
        else if(dist(0) > newDist(0) && dist(1) > newDist(1))
        {
          newDist
        }
        else {
          newDist
        }
      },

      triplet => {
        //println(triplet.srcId + " | " + triplet.srcAttr(0) + " | " + triplet.srcAttr(1) + " | " + triplet.attr + " | "+ triplet.dstAttr(0) + " | "+ triplet.dstAttr(1) + " | " + triplet.dstId)

        //send message to next node iff the new message we send to them could result in vertex updation else ignore sending
        if(triplet.srcId == srcId)
        {
          if(triplet.srcAttr(0) + triplet.attr  < triplet.dstAttr(0))
            Iterator((triplet.dstId, Array(triplet.srcAttr(0) + triplet.attr, Double.PositiveInfinity, triplet.srcId)))
          else
            Iterator.empty
        }

        else if (triplet.srcAttr(0) + triplet.attr  < triplet.dstAttr(0) && triplet.srcAttr(1) + triplet.attr  < triplet.dstAttr(1)) {
          //println(triplet.srcId + " |@| " + triplet.srcAttr(0) +  " |@| " + triplet.srcAttr(1) + " |@| " + triplet.attr + " |@| "+ triplet.dstAttr(0) + " |@| " + triplet.dstAttr(1) + " |@| " + triplet.dstId)
          Iterator((triplet.dstId, Array(triplet.srcAttr(0) + triplet.attr, triplet.srcAttr(1) + triplet.attr, triplet.srcId)))
        }
        else if (triplet.srcAttr(0) + triplet.attr  < triplet.dstAttr(0) && triplet.srcAttr(1) + triplet.attr  <= triplet.dstAttr(1)) {
          //println(triplet.srcId + " |@| " + triplet.srcAttr(0) +  " |@| " + triplet.srcAttr(1) + " |@| " + triplet.attr + " |@| "+ triplet.dstAttr(0) + " |@| " + triplet.dstAttr(1) + " |@| " + triplet.dstId)
          Iterator((triplet.dstId, Array(triplet.srcAttr(0) + triplet.attr, triplet.dstAttr(1), triplet.srcId)))
        }
        else if (triplet.srcAttr(0) + triplet.attr  < triplet.dstAttr(0) && triplet.srcAttr(1) + triplet.attr  > triplet.dstAttr(0)) {
          //println(triplet.srcId + " |#| " + triplet.srcAttr(0) +  " |#| " + triplet.srcAttr(1) + " |#| " + triplet.attr + " |#| "+ triplet.dstAttr(0) + " |#| " + triplet.dstAttr(1) + " |#| " + triplet.dstId)
          Iterator((triplet.dstId, Array(triplet.srcAttr(0) + triplet.attr, triplet.dstAttr(0), triplet.srcId)))
        }
        else if (triplet.srcAttr(0) + triplet.attr   > triplet.dstAttr(0) && triplet.srcAttr(0) + triplet.attr  < triplet.dstAttr(1)) {
          //println(triplet.srcId + " |$| " + triplet.srcAttr(0) +  " |$| " + triplet.srcAttr(1) + " |$| " + triplet.attr + " |$| "+ triplet.dstAttr(0) + " |$| " + triplet.dstAttr(1) + " |$| " + triplet.dstId)
          Iterator((triplet.dstId, Array(triplet.dstAttr(0), triplet.srcAttr(0) + triplet.attr, triplet.srcId)))
        }
        else {
          Iterator.empty
        }

      },

      (a, b) => {
        //println("source Id is: " + srcId)
        //println(a.mkString(" ---a--- "))
        //println(b.mkString(" ---b--- "))
        //Merge ,essage arriving the the vertex
        if (a(0) < b(0) && a(1) < b(1))
        { a }
        else if (a(0) < b(0) && a(1) >= b(1))
        { Array(a(0), b(1), a(2), b(3)) }
        else if (a(0) >= b(0) && a(1) < b(1))
        { Array(b(0), a(1), b(2), a(3)) }
        else if (a(0) >= b(0) && a(1) >= b(1))
        { b }
        else
        { b }
      }
    )

    //val ans2 = msssp.vertices.map(vertex => "Shortest distances from Source Vertex " + srcId + " to target landmark vertex: " + vertex._1 + " are  " + vertex._2(0) + " | " + vertex._2(1))
    //ans2.take(10).foreach(println)
    val q = msssp.vertices.filter(vertex => vertex._1 == tgtId)
    val z = q.take(5)
    if(z.length == 0){
      return (srcId, tgtId, Double.PositiveInfinity, Double.PositiveInfinity)
    }
    println("the length of z is: " + z.length + " for source and dst id: " + srcId + " , " + tgtId + " | " + z(0)._2(0) + " , " + z(0)._2(1))
    val f = Array(srcId , tgtId, z(0)._2(0), z(0)._2(1))

    return (srcId , tgtId, z(0)._2(0).asInstanceOf[Double], z(0)._2(1).asInstanceOf[Double])
  }

}
