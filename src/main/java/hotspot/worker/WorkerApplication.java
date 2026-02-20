package hotspot.worker;

import java.util.Arrays;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import hotspot.seed.SeedRunner;

@EnableScheduling
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
