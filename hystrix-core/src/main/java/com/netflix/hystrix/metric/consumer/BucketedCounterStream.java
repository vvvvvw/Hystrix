/**
 * Copyright 2015 Netflix, Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.hystrix.metric.consumer;

import com.netflix.hystrix.metric.HystrixEvent;
import com.netflix.hystrix.metric.HystrixEventStream;
import rx.Observable;
import rx.Subscription;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.subjects.BehaviorSubject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract class that imposes a bucketing structure and provides streams of buckets
 *
 * @param <Event> type of raw data that needs to get summarized into a bucket
 * @param <Bucket> type of data contained in each bucket
 * @param <Output> type of data emitted to stream subscribers (often is the same as A but does not have to be)
 */
//提供了基本的桶计数器实现
    //泛型里接受三个类型参数：Event 类型代表 Hystrix 中的调用事件，如命令开始执行、命令执行完成等
    //第二个 Bucket 类型代表桶的类型，第三个 Output 类型代表数据聚合的最终输出类型
public abstract class BucketedCounterStream<Event extends HystrixEvent, Bucket, Output> {
    //滑动窗口中桶的个数
    protected final int numBuckets;
    protected final Observable<Bucket> bucketedStream;
    protected final AtomicReference<Subscription> subscription = new AtomicReference<Subscription>(null);

    private final Func1<Observable<Event>, Observable<Bucket>> reduceBucketToSummary;

    private final BehaviorSubject<Output> counterSubject = BehaviorSubject.create(getEmptyOutputValue());

    protected BucketedCounterStream(final HystrixEventStream<Event> inputEventStream, final int numBuckets, final int bucketSizeInMs,
                                    final Func2<Bucket, Event, Bucket> appendRawEventToBucket) {
        //桶的数量
        this.numBuckets = numBuckets;
        this.reduceBucketToSummary = new Func1<Observable<Event>, Observable<Bucket>>() {
            @Override
            public Observable<Bucket> call(Observable<Event> eventBucket) {
                //将事件收集为桶
                return eventBucket.reduce(getEmptyBucketSummary(), appendRawEventToBucket);
            }
        };

        final List<Bucket> emptyEventCountsToStart = new ArrayList<Bucket>();
        for (int i = 0; i < numBuckets; i++) {
            emptyEventCountsToStart.add(getEmptyBucketSummary());
        }

        //本次得到的数据流（类型为 RxJava 中的 Observable，即观察者模式中的 Publisher，
        // 会源源不断地产生事件/数据），里面最核心的逻辑就是如何将一个一个的事件按一段时间聚合成一个桶
        this.bucketedStream = Observable.defer(new Func0<Observable<Bucket>>() {
            @Override
            public Observable<Bucket> call() {
                return inputEventStream
                        .observe()
                        // 按单元窗口长度来将某个时间段内的调用事件聚集起来
                        //每个桶对应的窗口长度就是 bucketSizeInMs = timeInMilliseconds / numBuckets（记为一个单元窗口周期）
                        //每隔一个单元窗口周期（bucketSizeInMs）就把这段时间内的所有调用事件聚合到一个桶内
                        .window(bucketSizeInMs, TimeUnit.MILLISECONDS) //bucket it by the counter window so we can emit to the next operator in time chunks, not on every OnNext
                        //按单元窗口长度来将某个时间段内的调用事件聚集起来，此时数据流里每个对象都是一个集合：Observable<Event>，所以需要将其聚集成桶类型以将其扁平化。Hystrix 通过 RxJava 的 reduce 操作符进行“归纳”操作，将一串事件归纳成一个桶
                        // 将每个单元窗口内聚集起来的事件集合聚合成桶
                        .flatMap(reduceBucketToSummary)                //for a given bucket, turn it into a long array containing counts of event types
                        // 为了保证窗口的完整性，开始的时候先产生一串空的桶
                        //先填满足够数量的空桶
                        .startWith(emptyEventCountsToStart);           //start it with empty arrays to make consumer logic as generic as possible (windows are always full)
            }
        });
    }

    abstract Bucket getEmptyBucketSummary();

    abstract Output getEmptyOutputValue();

    /**
     * Return the stream of buckets
     * @return stream of buckets
     */
    public abstract Observable<Output> observe();

    public void startCachingStreamValuesIfUnstarted() {
        if (subscription.get() == null) {
            //the stream is not yet started
            Subscription candidateSubscription = observe().subscribe(counterSubject);
            if (subscription.compareAndSet(null, candidateSubscription)) {
                //won the race to set the subscription
            } else {
                //lost the race to set the subscription, so we need to cancel this one
                candidateSubscription.unsubscribe();
            }
        }
    }

    /**
     * Synchronous call to retrieve the last calculated bucket without waiting for any emissions
     * @return last calculated bucket
     */
    public Output getLatest() {
        startCachingStreamValuesIfUnstarted();
        if (counterSubject.hasValue()) {
            return counterSubject.getValue();
        } else {
            return getEmptyOutputValue();
        }
    }

    public void unsubscribe() {
        Subscription s = subscription.get();
        if (s != null) {
            s.unsubscribe();
            subscription.compareAndSet(s, null);
        }
    }
}
