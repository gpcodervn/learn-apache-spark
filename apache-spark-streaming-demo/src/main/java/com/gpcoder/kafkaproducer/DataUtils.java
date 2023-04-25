package com.gpcoder.kafkaproducer;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataUtils {

    private static final Object[][] INIT_DATA =  new Object[][]{
            {0, "Spring Boot Microservices"},
            {1, "Spring Framework Fundamentals"},
            {2, "Spring JavaConfig"},
            {3, "Spring MVC and WebFlow"},
            {4, "JavaEE and WildFly Module 1 : Getting Started"},
            {5, "Hibernate and JPA"},
            {6, "Java Web Development Second Edition: Module 1"},
            {7, "Java Fundamentals"},
            {8, "NoSQL Databases"},
            {9, "Java Advanced Topics"},
            {10, "Docker for Java Developers"},
            {11, "Java Web Development Second Edition: Module 2"},
            {12, "HTML5 and Responsive CSS for Developers"},
            {13, "Git"},
            {14, "Spring Boot"},
            {15, "Groovy Programming"},
            {16, "Java Build Tools"},
            {17, "Hadoop for Java Developers"},
            {18, "Cloud Deployment with AWS"},
            {19, "Docker Module 2 for Java Developers"},
            {20, "Going Further with Android"},
            {21, "Test Driven Development"},
            {22, "Introduction to Android"},
            {23, "Java Web Development"},
            {24, "Spring Security Module 3"},
            {25, "Java Messaging with JMS and MDB"},
            {26, "Spring Remoting and Webservices"},
            {27, "Thymeleaf"},
            {28, "Spring Security Module 2: OAuth2 and REST"},
            {29, "JavaEE and WildFly Module 2: Webservices"},
            {30, "Spring Security Core Concepts"},
            {31, "JavaEE and Wildfly Module 3: Messaging"},
            {32, "JavaEE"},
            {33, "Microservice Deployment"},
            {34, "Securing a VPC"},
            {35, "WTP Plugins for Eclipse"},
            {36, "Spark for Java Developers"},
            {37, "JavaEE and Wildfly Module 4: JSF"},
            {38, "Kubernetes Microservices Module 1"},
            {39, "Kotlin with Spring Boot"},
            {40, "Kubernetes Microservices Module 2"},
            {41, "Spark Module 2 SparkSQL and DataFrames"},
            {42, "Spark Module 3 Machine Learning SparkML"}};

    public static Map<Integer, String> initCourseKeys() {
        return Stream.of(INIT_DATA)
                .collect(Collectors.toMap(it -> (Integer) it[0], it -> (String) it[1]));
    }

    private DataUtils() {}
}
