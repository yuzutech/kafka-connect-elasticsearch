package fr.yuzutech.kafka.connect.elasticsearch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import rx.Subscriber;

public class ReactiveBufferTest {

    @Test
    public void should_flush_every_10_values_or_100ms() throws InterruptedException {
        ReactiveBuffer reactiveBuffer = new ReactiveBuffer(100, TimeUnit.MILLISECONDS, 10);
        final List<List<String>> batchs = new ArrayList<>();
        reactiveBuffer.getBuffer().subscribe(new Subscriber<List<String>>() {
            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onNext(List<String> data) {
                batchs.add(data);
            }
        });
        reactiveBuffer.put("1");
        reactiveBuffer.put("2");
        reactiveBuffer.put("3");
        reactiveBuffer.put("4");
        reactiveBuffer.put("5");
        reactiveBuffer.put("6");
        reactiveBuffer.put("7");
        reactiveBuffer.put("8");
        reactiveBuffer.put("9");
        reactiveBuffer.put("10");
        reactiveBuffer.put("11");
        Thread.sleep(110);
        reactiveBuffer.put("12");
        reactiveBuffer.put("13");
        reactiveBuffer.put("14");
        Thread.sleep(110);
        reactiveBuffer.put("15");
        reactiveBuffer.onCompleted();
        assertThat(batchs).hasSize(4);
        assertThat(batchs.get(0)).containsExactly("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
        assertThat(batchs.get(1)).containsExactly("11");
        assertThat(batchs.get(2)).containsExactly("12", "13", "14");
        assertThat(batchs.get(3)).containsExactly("15");
    }
}
