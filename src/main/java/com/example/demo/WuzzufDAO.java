/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.demo;

import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.*;
import org.apache.spark.sql.Encoders;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.style.Styler;
import java.io.IOException;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.types.StructType;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.sql.*;

/**
 *
 * @author MA
 */
public class WuzzufDAO {
    String path ="src/main/resources/Wuzzuf_Jobs.csv";
    SparkSession sparkSession = SparkSession.builder ().appName ("Spark CSV Analysis Demo").master ("local[2]")
                .getOrCreate ();
    // *** Read data ***//
    public Dataset<Row> readDataSet(){
        DataFrameReader dataFrameReader = sparkSession.read ();
        dataFrameReader.option ("header", "true");
        Dataset<Row> csvDataFrame = dataFrameReader.csv (path);
        csvDataFrame.toDF();
       return csvDataFrame;
    }
    Dataset<Row> data =readDataSet();
     // *** display data ***//
     public String displayData(){
        List<Row> first_20_records = data.limit(20).collectAsList();
        return DisplayHtml.displayrows(data.columns(), first_20_records);
    }

      // *** Clean data ***//
    public Dataset<Row> cleanDataSet(){
        data.na().drop("all");
        Dataset<Row> Data=data.distinct();
       return Data;     
    }
    Dataset<Row> cleanData =cleanDataSet();
      // *** display structure of the data ***//
     public String structure(){
        StructType dataStructure = cleanData.schema();
        return dataStructure.prettyJson();
    }
     // *** display summary of the data ***//
      public String summary() {
        Dataset<Row> data_summary = cleanData.describe(new String[]{"Title","Company","Location","Type","Level","YearsExp","Country","Skills"});
        List<Row> summary = data_summary.collectAsList();
        return DisplayHtml.displayrows(data_summary.columns(), summary);
    }
       // *** Count the jobs for each company ***//
       public String countJobsperCompany(){
        cleanData.createOrReplaceTempView ("Wuzzuf_Jobs");
        Dataset<Row> jobsperCompany= sparkSession.sql ("SELECT Company,COUNT(*)  FROM Wuzzuf_Jobs GROUP BY Company ORDER BY COUNT(*) DESC  ").limit(20); 
        List<Row> topCompanies = jobsperCompany.collectAsList();
        return DisplayHtml.displayrows(jobsperCompany.columns(), topCompanies);
    }
       // *** JobsperCompany Chart ***//
       public String getCompanyPieChart() throws IOException
    {
      cleanData.createOrReplaceTempView ("Wuzzuf_Jobs");
      Dataset<Row> jobsperCompany= sparkSession.sql ("SELECT Company,COUNT(*)  FROM Wuzzuf_Jobs GROUP BY Company ORDER BY COUNT(*) DESC  "); 
      List<String> company= jobsperCompany.select("Company").limit(10).as(Encoders.STRING()).collectAsList();
      List<Long> count= jobsperCompany.select("count(1)").limit(10).as(Encoders.LONG()).collectAsList();
      Map<String,Long> result = new HashMap<>(); 
      for (int i=0; i<company.size(); i++) {
      result.put(company.get(i),  count.get(i));    
    }
     PieChart chart = new PieChartBuilder().width (800).height (600).title("JobsperCompany Chart").build ();
     for (int i=0; i<company.size(); i++) {
         chart.addSeries(company.get(i), result.get(company.get(i)));    
     }
      String path1 = "src\\main\\resources\\Sample_pieChart.png";
      BitmapEncoder.saveBitmap(chart,path1, BitmapEncoder.BitmapFormat.PNG);
      return DisplayHtml.viewchart(path1);
    }

    
    public String getPopularJobTitles (){
        cleanData.createOrReplaceTempView ("Wuzzuf_jobs");
        Dataset<Row> jobTitles= sparkSession.sql("SELECT Title, COUNT(*)"
                + " FROM Wuzzuf_jobs "
                + "GROUP BY Title "
                + "ORDER BY COUNT(*) DESC").limit(20);
        List<Row> top_titles = jobTitles.collectAsList();
        return DisplayHtml.displayrows(jobTitles.columns(), top_titles);
    }
    public String graphJobTitles()throws IOException{
    
        cleanData.createOrReplaceTempView ("Wuzzuf_jobs");
        Dataset<Row> jobTitles= sparkSession.sql("SELECT Title, COUNT(*)"
                + " FROM Wuzzuf_jobs "
                + "GROUP BY Title "
                + "ORDER BY COUNT(*) DESC");
        List<String> titles =  jobTitles.select("Title").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> weight = jobTitles.select("count(1)").limit(10).as(Encoders.LONG()).collectAsList();
        CategoryChart chart = new CategoryChartBuilder().width(1024).height(768).title("Most popular titles").xAxisTitle("Titles").yAxisTitle("frequency").build();
        chart.getStyler().setXAxisLabelRotation(45);
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);
        chart.addSeries("Titles",titles, weight);
        String path2 = "src\\main\\resources\\Titles_barChart.png";
        BitmapEncoder.saveBitmap(chart,path2, BitmapEncoder.BitmapFormat.PNG);
        return DisplayHtml.viewchart(path2);
    }
    public String getPopularAreas (){
        
        cleanData.createOrReplaceTempView ("Wuzzuf_jobs");
        Dataset<Row> areas= sparkSession.sql("SELECT Location, COUNT(*)"
                + " FROM Wuzzuf_jobs "
                + "GROUP BY Location "
                + "ORDER BY COUNT(*) DESC").limit(20);
        List<Row> top_titles = areas.collectAsList();
        return DisplayHtml.displayrows(areas.columns(), top_titles);
    }
    public String graphAreas()throws IOException{
        
        cleanData.createOrReplaceTempView ("Wuzzuf_jobs");
        Dataset<Row> popularAreas= sparkSession.sql("SELECT Location, COUNT(*)"
                + " FROM Wuzzuf_jobs "
                + "GROUP BY Location "
                + "ORDER BY COUNT(*) DESC");
        List<String> areas=  popularAreas.select("Location").limit(10).as(Encoders.STRING()).collectAsList();
        List<Long> weight = popularAreas.select("count(1)").limit(10).as(Encoders.LONG()).collectAsList();
        CategoryChart chart = new CategoryChartBuilder().width(1024).height(768).title("Most popular areas").xAxisTitle("Areas").yAxisTitle("frequancy").build();
        chart.getStyler().setXAxisLabelRotation(45);
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked(true);
        chart.addSeries("Areas", areas, weight);
        String path3 = "src\\main\\resources\\Location_barChart.png";
        BitmapEncoder.saveBitmap(chart,path3, BitmapEncoder.BitmapFormat.PNG);
        return DisplayHtml.viewchart(path3);
    }
       // *** skills ***//
    public String skills(){
        cleanData.createOrReplaceTempView ("Wuzzuf_Jobs");
        Dataset<Row> skills1=sparkSession.sql("SELECT explode(split(Skills, ','))as t_skills  FROM Wuzzuf_Jobs ");
        skills1.createOrReplaceTempView ("skills_Jobs");
        Dataset<Row> skills = sparkSession.sql("SELECT t_skills,count(t_skills)   FROM skills_Jobs GROUP by t_skills ORDER BY COUNT(t_skills) DESC").limit(20);
        List<Row> topSkills = skills.collectAsList();
        return DisplayHtml.displayrows(skills.columns(), topSkills);
    }
       
    public String FactorizeYearsExp (){
        StringIndexer indexer = new StringIndexer()
        .setInputCol("YearsExp")
        .setOutputCol("factorizedYearsExp");
        Dataset<Row> indexed = indexer.fit(cleanData).transform(cleanData);
        String columns[] = {"YearsExp", "factorizedYearsExp"};
        List<Row>indexedYearExp = indexed.select("YearsExp","factorizedYearsExp").limit(20).collectAsList();
        return DisplayHtml.displayrows(columns, indexedYearExp);
    }
    public String  kmeanTitleCompany(){
         
         StringIndexer indexer = new StringIndexer().setInputCol("Title").setOutputCol("TitleIndex");
         cleanData =indexer.fit(cleanData).transform(cleanData);
         StringIndexer indexer1 = new StringIndexer().setInputCol("Company").setOutputCol("CompanyIndex");
         cleanData =indexer1.fit(cleanData).transform(cleanData);
         VectorAssembler vectorAssembler = new VectorAssembler()
                                           .setInputCols(new String[]{"TitleIndex","CompanyIndex"})
                                           .setOutputCol("features");
         Dataset<Row> featureData=vectorAssembler.transform(cleanData).select("features");
         KMeans kMeans=new KMeans().setK(9).setSeed(1L);
         KMeansModel model = kMeans.fit(featureData); 
         int iterations = model.getMaxIter();
         Dataset<Row> predictions=model.transform(featureData);
         predictions.createOrReplaceTempView ("predictions");
         Dataset<Row> c1usterPrediction= sparkSession.sql ("SELECT prediction,COUNT(*)  FROM predictions GROUP BY prediction ORDER BY COUNT(*) DESC  ");
        List<Row> cluster = c1usterPrediction.collectAsList();
        String centers = Arrays.toString(model.clusterCenters());
        return "The total number of iterations : " + String.valueOf(iterations )+DisplayHtml.displayrows(c1usterPrediction.columns(), cluster)+"centers : " +centers;
         
     }
    

}
