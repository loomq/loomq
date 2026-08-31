package com.loomq.application.scheduler;

import com.loomq.spi.IntentObserver;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Intent 生命周期观察器列表(线程安全);单 observer 异常不影响其他 observer 与调度循环。 */
final class ObserverNotifier {

    private static final Logger logger = LoggerFactory.getLogger(ObserverNotifier.class);

    private final List<IntentObserver> observers = new CopyOnWriteArrayList<>();

    void setObservers(List<IntentObserver> observers) {
        this.observers.clear();
        if (observers != null) {
            this.observers.addAll(observers);
        }
    }

    void add(IntentObserver observer) {
        if (observer != null) {
            observers.add(observer);
        }
    }

    void remove(IntentObserver observer) {
        observers.remove(observer);
    }

    boolean isEmpty() {
        return observers.isEmpty();
    }

    void notifyObservers(Consumer<IntentObserver> action) {
        for (IntentObserver o : observers) {
            try {
                action.accept(o);
            } catch (Exception e) {
                logger.error("Observer error", e);
            }
        }
    }
}
