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
import rx.functions.Action0;
import rx.functions.Func1;
import rx.functions.Func2;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Refinement of {@link BucketedCounterStream} which reduces numBuckets at a time.
 *
 * @param <Event>  type of raw data that needs to get summarized into a bucket
 * @param <Bucket> type of data contained in each bucket
 * @param <Output> type of data emitted to stream subscribers (often is the same as A but does not have to be)
 */
//按照滑动窗口的大小对每个单元窗口产生的桶进行聚合
public abstract class BucketedRollingCounterStream<Event extends HystrixEvent, Bucket, Output> extends BucketedCounterStream<Event, Bucket, Output> {
    private Observable<Output> sourceStream;
    private final AtomicBoolean isSourceCurrentlySubscribed = new AtomicBoolean(false);

    protected BucketedRollingCounterStream(HystrixEventStream<Event> stream, final int numBuckets, int bucketSizeInMs,
                                           //将事件流聚合成桶
                                           final Func2<Bucket, Event, Bucket> appendRawEventToBucket,
                                           //将桶聚合成输出对象
                                           final Func2<Output, Bucket, Output> reduceBucket) {
        super(stream, numBuckets, bucketSizeInMs, appendRawEventToBucket);
        Func1<Observable<Bucket>, Observable<Output>> reduceWindowToSummary = new Func1<Observable<Bucket>, Observable<Output>>() {
            @Override
            public Observable<Output> call(Observable<Bucket> window) {
                //每个集合的大小都是 numBuckets，看起来用 reduce 和 scan + skip(numBuckets)
                // 没有什么区别，但是注意当数据流开始的时候，最后面的窗口大小都不满 numBuckets，
                // todo 这时候就需要把这些不完整的窗口给过滤掉来确保数据不缺失。
                // 这个地方也是开发的时候容易忽略的地方
                return window.scan(getEmptyOutputValue(), reduceBucket).skip(numBuckets);
            }
        };
        // 数据流，每个对象代表单元窗口产生的桶
        this.sourceStream = bucketedStream      //stream broken up into buckets
                // 按照滑动窗口桶的个数进行桶的聚集
                .window(numBuckets, 1)          //emit overlapping windows of buckets
                // 将一系列的桶聚集成最后的数据对象
                .flatMap(reduceWindowToSummary) //convert a window of bucket-summaries into a single summary
                .doOnSubscribe(new Action0() {
                    @Override
                    public void call() {
                        isSourceCurrentlySubscribed.set(true);
                    }
                })
                .doOnUnsubscribe(new Action0() {
                    @Override
                    public void call() {
                        isSourceCurrentlySubscribed.set(false);
                    }
                })
                // 不同的订阅者看到的数据是一致的
                .share()                        //multiple subscribers should get same data
                // 流量控制，当消费者消费速度过慢时就丢弃数据，不进行积压
                .onBackpressureDrop();          //if there are slow consumers, data should not buffer
    }

    @Override
    public Observable<Output> observe() {
        return sourceStream;
    }

    /* package-private */ boolean isSourceCurrentlySubscribed() {
        return isSourceCurrentlySubscribed.get();
    }
}
