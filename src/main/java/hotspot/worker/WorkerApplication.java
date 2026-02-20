package hotspot.worker;

import java.util.Arrays;
import hotspot.seed.SeedRunner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

//@EnableScheduling
@SpringBootApplication
public class WorkerApplication {

    public static void main(String[] args) {
    if (Arrays.asList(args).contains("seed")) {
        SpringApplication.run(SeedRunner.class, args);
    } else {
        SpringApplication.run(WorkerApplication.class, args);
    }
}

}
