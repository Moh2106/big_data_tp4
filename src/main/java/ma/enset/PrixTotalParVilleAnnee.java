package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class PrixTotalParVilleAnnee {
    public static void main(String[] args) {
//      Configuration de spark
        SparkConf sparkConf = new SparkConf().setAppName("PrixTotalParVilleAnne").setMaster("local");
        SparkContext sc = new SparkContext(sparkConf);

//     Chargement du fichier vente.txt en RDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Mohammed\\IdeaProjects\\BigData\\tp4_exercice2\\src\\main\\java\\ma\\enset\\vente.txt", sc.defaultMinPartitions()).toJavaRDD();

//        Transformation pour obtenir un RDD de paire ville prix
        JavaPairRDD<String, Double> rddVillePrix = lines.mapToPair(line -> {
            String[] part = line.split(" ");
            return new Tuple2<>(part[1], Double.parseDouble(part[3]));
        });


//        Reduction pour obtenir le total des ventes par ville
        JavaPairRDD<String, Double> rddPrixTotalParVille = rddVillePrix.reduceByKey((a,b)-> a+b);

//        Afficher le résultat
        rddPrixTotalParVille.foreach(pair -> System.out.println(pair._1() + " : " + pair._2()));

//        Arretter le contexte spark
        sc.stop();
    }
}
