package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class PrixTotalParProduitAnnee {
    public static void main(String[] args) {
//        Configuration de Spark
        SparkConf sparkConf = new SparkConf().setAppName("PrixTotalParProduitAnnee").setMaster("local");
        SparkContext sc = new SparkContext(sparkConf);

//        Chargement du fichier vente.txt
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Mohammed\\IdeaProjects\\BigData\\tp4_exercice2\\src\\main\\java\\ma\\enset\\vente.txt", sc.defaultMinPartitions()).toJavaRDD();

//        Extraire les ventes des produits des villes pour une année donné
        JavaRDD<String> rddProduitsVillesAnnee = lines.filter(line -> line.contains("2023"));

//        Extraire les villes et les prix des produits
        JavaPairRDD<String, Double> rddVillePrix = rddProduitsVillesAnnee.mapToPair(line -> {
            String[] result = line.split(" ");
            return new Tuple2<>(result[1],Double.parseDouble(result[3]));
        });

//        Calculer le prix total par ville
        JavaPairRDD<String, Double>  rddPrixTotal = rddVillePrix.reduceByKey((a,b) -> a+b);

//        Afficher le résultat final
        rddPrixTotal.foreach(pair -> System.out.println(pair._1() + " : " + pair._2()));

//        Arreter le contexte spark
        sc.stop();

    }
}
