/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class WuzzufController {
    WuzzufDAO service = new WuzzufDAO();
    @GetMapping("/show_first_records")
    public  String  show_first_records(){
        return service.displayData();
    }

    @GetMapping("/show_structure")
    public  String  show_structure(){
        return service.structure();
    }


    @GetMapping("/show_summary")
    public  String  show_summary(){
        return service.summary();
    }


    @GetMapping("/show_top_companies")
    public  String  show_top_companies(){
        return service.countJobsperCompany();
    }

    @GetMapping("/show_top_titles")
    public  String  show_top_titles(){
        return service.getPopularJobTitles ();
    }

    @GetMapping("/show_top_areas")
    public  String  show_top_countries(){
        return service.getPopularAreas ();
    }

    @GetMapping("/show_pie_chart")
    public  String  show_pie_chart() throws IOException {
        return service.getCompanyPieChart();
    }

    @GetMapping("/title_bar_chart")
    public  String  title_bar_chart() throws IOException {
        return service.graphJobTitles();
    }

    @GetMapping("/location_bar_chart")
    public  String  location_bar_chart() throws IOException {
        return service.graphAreas();
    }

    @GetMapping("/show_top_skills")
    public String show_top_skills() throws IOException {
        return service.skills();
    }

    @GetMapping("/show_YearsExp")
    public  String  show_YearsExp() throws IOException {
        return service.FactorizeYearsExp ();
    }

    @GetMapping("/kMeans")
    public  String kMeans() throws IOException {
        return service.kmeanTitleCompany();
    }

}