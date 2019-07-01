/**
 * Copyright 2015 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.hystrix.metric;

import com.netflix.hystrix.HystrixCommandKey;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-Command stream of {@link HystrixCommandCompletion}s.  This gets written to by {@link HystrixThreadEventStream}s.
 * Events are emitted synchronously in the same thread that performs the command execution.
 */
public class HystrixCommandCompletionStream implements HystrixEventStream<HystrixCommandCompletion> {
    private final HystrixCommandKey commandKey;

    private final Subject<HystrixCommandCompletion, HystrixCommandCompletion> writeOnlySubject;
    private final Observable<HystrixCommandCompletion> readOnlyStream;

    private static final ConcurrentMap<String, HystrixCommandCompletionStream> streams = new ConcurrentHashMap<String, HystrixCommandCompletionStream>();

    //若对应的 CommandKey 的事件流已创建就从缓存中取出，否则就新创建并缓存起来，保证每个 CommandKey 只有一个实例
    public static HystrixCommandCompletionStream getInstance(HystrixCommandKey commandKey) {
        HystrixCommandCompletionStream initialStream = streams.get(commandKey.name());
        if (initialStream != null) {
            return initialStream;
        } else {
            synchronized (HystrixCommandCompletionStream.class) {
                HystrixCommandCompletionStream existingStream = streams.get(commandKey.name());
                if (existingStream == null) {
                    HystrixCommandCompletionStream newStream = new HystrixCommandCompletionStream(commandKey);
                    streams.putIfAbsent(commandKey.name(), newStream);
                    return newStream;
                } else {
                    return existingStream;
                }
            }
        }
    }

    HystrixCommandCompletionStream(final HystrixCommandKey commandKey) {
        this.commandKey = commandKey;

        this.writeOnlySubject = new SerializedSubject<HystrixCommandCompletion, HystrixCommandCompletion>(PublishSubject.<HystrixCommandCompletion>create());
        this.readOnlyStream = writeOnlySubject.share();
    }

    public static void reset() {
        streams.clear();
    }

    /*
    write 方法里通过向某个 Subject 发布事件来实现了发布的逻辑，那么 Subject 又是什么呢？
    简单来说，Subject 就像是一个桥梁，既可以作为发布者 Observable，
    又可以作为订阅者 Observer。它可以作为发布者和订阅者之间的一个“代理”，
    提供额外的功能（如流量控制、缓存等）。这里的 writeOnlySubject 是经过 SerializedSubject 封装的 PublishSubject。PublishSubject 可以看做 hot observable。为了保证调用的顺序（根据 The Observable Contract，每个事件的产生需要满足顺序上的偏序关系，即使是在不同线程产生），
    需要用 SerializedSubject 封装一层来保证事件真正地串行地产生
     */
    public void write(HystrixCommandCompletion event) {
        writeOnlySubject.onNext(event);
    }


    @Override
    public Observable<HystrixCommandCompletion> observe() {
        return readOnlyStream;
    }

    @Override
    public String toString() {
        return "HystrixCommandCompletionStream(" + commandKey.name() + ")";
    }
}
