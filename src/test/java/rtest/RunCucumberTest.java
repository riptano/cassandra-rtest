package rtest;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

@RunWith(Cucumber.class)
@CucumberOptions(plugin = {"pretty", "junit:target/surefire-reports/junit.xml", "html:target/surefire-reports/cucumber-report.html"})
public class RunCucumberTest {

}
