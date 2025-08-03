package net.kacemi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class App1VentesParVille {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("total des ventes par ville").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lignes = sc.textFile("ventes.txt");

        JavaPairRDD<String, Double> ventesParVille = lignes.mapToPair(ligne -> {
            String[] champs = ligne.split(" ");
            String ville = champs[1];
            Double prix = Double.parseDouble(champs[3]);
            return new Tuple2<>(ville, prix);
        });

        JavaPairRDD<String, Double> totalParVille = ventesParVille.reduceByKey((prix1, prix2) -> prix1 + prix2);


        List<Tuple2<String, Double>> resultats = totalParVille.collect();
        for (Tuple2<String, Double> resultat : resultats) {
            System.out.println("Ville: " + resultat._1() + ", Total des ventes: " + String.format("%.2f", resultat._2()));
        }

        sc.close();


    }
}