package com.nekolr;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class SequenceFkApplicationTests {

    @Autowired
    private SequenceFuckOffRunner sequenceFuckOffRunner;

    @Test
    void contextLoads() {
        sequenceFuckOffRunner.run("resource");
    }

}
