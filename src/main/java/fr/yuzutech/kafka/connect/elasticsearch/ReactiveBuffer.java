package fr.yuzutech.kafka.connect.elasticsearch;

import java.util.List;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.subjects.PublishSubject;

public class ReactiveBuffer {

    private PublishSubject<String> publishSubject;
    private Observable<List<String>> buffer;

    public ReactiveBuffer(long timespan, TimeUnit timeUnit, int count) {
        this.publishSubject = PublishSubject.create();
        this.buffer = this.publishSubject.buffer(timespan, timeUnit, count);
    }

    public Observable<List<String>> getBuffer() {
        return buffer;
    }

    public void put(String value) {
        publishSubject.onNext(value);
    }

    public void onCompleted() {
        publishSubject.onCompleted();
    }
}
