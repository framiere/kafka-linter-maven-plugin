package sample;

import org.eclipse.microprofile.reactive.messaging.Incoming;

public final class BadIncoming {

    // RULE: QK_BLOCKING_MISSING_ON_BLOCKING_LISTENER — calls Thread.sleep with no @Blocking.
    @Incoming("orders")
    public void onMessage(String body) throws InterruptedException {
        Thread.sleep(50);
        System.out.println(body);
    }
}
