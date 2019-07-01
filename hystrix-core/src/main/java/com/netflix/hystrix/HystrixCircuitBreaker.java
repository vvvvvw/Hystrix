/**
 * Copyright 2012 Netflix, Inc.
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
package com.netflix.hystrix;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.hystrix.HystrixCommandMetrics.HealthCounts;
import rx.Subscriber;
import rx.Subscription;

/**
 * Circuit-breaker logic that is hooked into {@link HystrixCommand} execution and will stop allowing executions if failures have gone past the defined threshold.
 * <p>
 * The default (and only) implementation  will then allow a single retry after a defined sleepWindow until the execution
 * succeeds at which point it will again close the circuit and allow executions again.
 */
public interface HystrixCircuitBreaker {

    /**
     * Every {@link HystrixCommand} requests asks this if it is allowed to proceed or not.  It is idempotent and does
     * not modify any internal state, and takes into account the half-open logic which allows some requests through
     * after the circuit has been opened
     * 
     * @return boolean whether a request should be permitted
     */
    //幂等操作每个Hystrix命令的请求都通过它来判断是否可以被执行，包括半开逻辑
    boolean allowRequest();

    /**
     * Whether the circuit is currently open (tripped).
     * 
     * @return boolean state of circuit breaker
     */
    //返回当前断路器是否打开
    boolean isOpen();

    /**
     * Invoked on successful executions from {@link HystrixCommand} as part of feedback mechanism when in a half-open state.
     */
    //在{@link HystrixCommand}成功执行时调用，作为半开状态下的反馈机制的一部分。
    void markSuccess();

    /**
     * Invoked on unsuccessful executions from {@link HystrixCommand} as part of feedback mechanism when in a half-open state.
     */
    //作为处于半开状态的反馈机制的一部分，从{@link HystrixCommand}执行失败时调用
    void markNonSuccess();

    //在命令执行开始时调用以尝试执行。 这是非幂等的 - 它可能会修改内部状态。
    /**
     * Invoked at start of command execution to attempt an execution.  This is non-idempotent - it may modify internal
     * state.
     */
    boolean attemptExecution();

    /**
     * @ExcludeFromJavadoc
     * @ThreadSafe
     */
    class Factory {
        //Map<HystrixCommandKey.name(), HystrixCircuitBreaker>
        // String is HystrixCommandKey.name() (we can't use HystrixCommandKey directly as we can't guarantee it implements hashcode/equals correctly)
        private static ConcurrentHashMap<String, HystrixCircuitBreaker> circuitBreakersByCommand = new ConcurrentHashMap<String, HystrixCircuitBreaker>();

        /**
         * Get the {@link HystrixCircuitBreaker} instance for a given {@link HystrixCommandKey}.
         * <p>
         * This is thread-safe and ensures only 1 {@link HystrixCircuitBreaker} per {@link HystrixCommandKey}.
         * 
         * @param key
         *            {@link HystrixCommandKey} of {@link HystrixCommand} instance requesting the {@link HystrixCircuitBreaker}
         * @param group
         *            Pass-thru to {@link HystrixCircuitBreaker}
         * @param properties
         *            Pass-thru to {@link HystrixCircuitBreaker}
         * @param metrics
         *            Pass-thru to {@link HystrixCircuitBreaker}
         * @return {@link HystrixCircuitBreaker} for {@link HystrixCommandKey}
         */
        public static HystrixCircuitBreaker getInstance(HystrixCommandKey key, HystrixCommandGroupKey group, HystrixCommandProperties properties, HystrixCommandMetrics metrics) {
            // this should find it for all but the first time
            HystrixCircuitBreaker previouslyCached = circuitBreakersByCommand.get(key.name());
            if (previouslyCached != null) {
                return previouslyCached;
            }

            // if we get here this is the first time so we need to initialize

            // Create and add to the map ... use putIfAbsent to atomically handle the possible race-condition of
            // 2 threads hitting this point at the same time and let ConcurrentHashMap provide us our thread-safety
            // If 2 threads hit here only one will get added and the other will get a non-null response instead.
            HystrixCircuitBreaker cbForCommand = circuitBreakersByCommand.putIfAbsent(key.name(), new HystrixCircuitBreakerImpl(key, group, properties, metrics));
            if (cbForCommand == null) {
                // this means the putIfAbsent step just created a new one so let's retrieve and return it
                return circuitBreakersByCommand.get(key.name());
            } else {
                // this means a race occurred and while attempting to 'put' another one got there before
                // and we instead retrieved it and will now return it
                return cbForCommand;
            }
        }

        /**
         * Get the {@link HystrixCircuitBreaker} instance for a given {@link HystrixCommandKey} or null if none exists.
         * 
         * @param key
         *            {@link HystrixCommandKey} of {@link HystrixCommand} instance requesting the {@link HystrixCircuitBreaker}
         * @return {@link HystrixCircuitBreaker} for {@link HystrixCommandKey}
         */
        public static HystrixCircuitBreaker getInstance(HystrixCommandKey key) {
            return circuitBreakersByCommand.get(key.name());
        }

        /**
         * Clears all circuit breakers. If new requests come in instances will be recreated.
         */
        /* package */static void reset() {
            circuitBreakersByCommand.clear();
        }
    }


    ////断路器具体实现
    /**
     * The default production implementation of {@link HystrixCircuitBreaker}.
     * 
     * @ExcludeFromJavadoc
     * @ThreadSafe
     */
    //master和其他分支的代码逻辑没有问题
        // 期间的
    /* package */class HystrixCircuitBreakerImpl implements HystrixCircuitBreaker {
        //断路器对应的 HystrixCommand 实例的属性对象
        private final HystrixCommandProperties properties;
        //用来让HystrixCommand记录各类度量指标的对象
        private final HystrixCommandMetrics metrics;

        enum Status {
            CLOSED, OPEN, HALF_OPEN;
        }

        //当前断路器是否打开的标识，默认false
        private final AtomicReference<Status> status = new AtomicReference<Status>(Status.CLOSED);
        //断路器打开的时间戳(todo 其他分支代码是 断路器打开的时间或者是最新一次试探流量发送的时间 )，如果为-1，表示断路器关闭
        private final AtomicLong circuitOpened = new AtomicLong(-1);
        private final AtomicReference<Subscription> activeSubscription = new AtomicReference<Subscription>(null);

        protected HystrixCircuitBreakerImpl(HystrixCommandKey key, HystrixCommandGroupKey commandGroup, final HystrixCommandProperties properties, HystrixCommandMetrics metrics) {
            this.properties = properties;
            this.metrics = metrics;

            //On a timer, this will set the circuit between OPEN/CLOSED as command executions occur
            Subscription s = subscribeToStream();
            activeSubscription.set(s);
        }

        private Subscription subscribeToStream() {
            /*
             * This stream will recalculate the OPEN/CLOSED status on every onNext from the health stream
             */
            return metrics.getHealthCountsStream()
                    .observe()
                    .subscribe(new Subscriber<HealthCounts>() {
                        @Override
                        public void onCompleted() {

                        }

                        @Override
                        public void onError(Throwable e) {

                        }

                        @Override
                        public void onNext(HealthCounts hc) {
                            // check if we are past the statisticalWindowVolumeThreshold
                            //如果请求数大于阈值
                            if (hc.getTotalRequests() < properties.circuitBreakerRequestVolumeThreshold().get()) {
                                // we are not past the minimum volume threshold for the stat window,
                                // so no change to circuit status.
                                // if it was CLOSED, it stays CLOSED
                                // if it was half-open, we need to wait for a successful command execution
                                // if it was open, we need to wait for sleep window to elapse
                            } else {
                                //如果失败率小于阈值
                                if (hc.getErrorPercentage() < properties.circuitBreakerErrorThresholdPercentage().get()) {
                                    //we are not past the minimum error threshold for the stat window,
                                    // so no change to circuit status.
                                    // if it was CLOSED, it stays CLOSED
                                    // if it was half-open, we need to wait for a successful command execution
                                    // if it was open, we need to wait for sleep window to elapse
                                } else {
                                    //设置断路状态
                                    // our failure rate is too high, we need to set the state to OPEN
                                    if (status.compareAndSet(Status.CLOSED, Status.OPEN)) {
                                        circuitOpened.set(System.currentTimeMillis());
                                    }
                                }
                            }
                        }
                    });
        }

        @Override
        public void markSuccess() {
            if (status.compareAndSet(Status.HALF_OPEN, Status.CLOSED)) {
                //如果处于半开状态，并且本次试探成功，重置统计数据
                //This thread wins the race to close the circuit - it resets the stream to start it over from 0
                metrics.resetStream();
                Subscription previousSubscription = activeSubscription.get();
                if (previousSubscription != null) {
                    previousSubscription.unsubscribe();
                }
                Subscription newSubscription = subscribeToStream();
                activeSubscription.set(newSubscription);
                circuitOpened.set(-1L);
            }
        }

        @Override
        public void markNonSuccess() {
            if (status.compareAndSet(Status.HALF_OPEN, Status.OPEN)) {
                //如果失败，重新设置为断路
                //This thread wins the race to re-open the circuit - it resets the start time for the sleep window
                circuitOpened.set(System.currentTimeMillis());
            }
        }

        @Override
        public boolean isOpen() {
            //设置强制打开断路器
            if (properties.circuitBreakerForceOpen().get()) {
                return true;
            }
            //设置强制关闭断路器
            if (properties.circuitBreakerForceClosed().get()) {
                return false;
            }
            return circuitOpened.get() >= 0;
        }

        @Override
        public boolean allowRequest() {
            if (properties.circuitBreakerForceOpen().get()) {
                return false;
            }
            if (properties.circuitBreakerForceClosed().get()) {
                return true;
            }
            if (circuitOpened.get() == -1) {
                return true;
            } else {
                if (status.get().equals(Status.HALF_OPEN)) {
                    return false;
                } else {
                    return isAfterSleepWindow();
                }
            }
        }

        private boolean isAfterSleepWindow() {
            final long circuitOpenTime = circuitOpened.get();
            final long currentTime = System.currentTimeMillis();
            final long sleepWindowTime = properties.circuitBreakerSleepWindowInMilliseconds().get();
            //当前时间 距离 上一次 断路器打开或者上一次测试流量 发送的时间>窗口时间，返回true
            return currentTime > circuitOpenTime + sleepWindowTime;
        }

        @Override
        public boolean attemptExecution() {
            //设置强制打开断路器
            if (properties.circuitBreakerForceOpen().get()) {
                return false;
            }
            //设置强制关闭断路器
            if (properties.circuitBreakerForceClosed().get()) {
                return true;
            }
            //当前 断路器是否打开
            if (circuitOpened.get() == -1) {
                return true;
            } else {
                if (isAfterSleepWindow()) {
                    //only the first request after sleep window should execute
                    //if the executing command succeeds, the status will transition to CLOSED
                    //if the executing command fails, the status will transition to OPEN
                    //if the executing command gets unsubscribed, the status will transition to OPEN
                    //
                    if (status.compareAndSet(Status.OPEN, Status.HALF_OPEN)) {
                        //设置为半开状态
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
    }

    ////没有任何操作的断路器实现
    /**
     * An implementation of the circuit breaker that does nothing.
     * 
     * @ExcludeFromJavadoc
     */
    /* package */static class NoOpCircuitBreaker implements HystrixCircuitBreaker {

        @Override
        public boolean allowRequest() {
            return true;
        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public void markSuccess() {

        }

        @Override
        public void markNonSuccess() {

        }

        @Override
        public boolean attemptExecution() {
            return true;
        }
    }

}
