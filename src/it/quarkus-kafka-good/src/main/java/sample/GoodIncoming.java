package sample;

import io.smallrye.reactive.messaging.annotations.Blocking;
import org.eclipse.microprofile.reactive.messaging.Incoming;

public final class GoodIncoming {

    @Incoming("orders")
    @Blocking
    public void onMessage(String body) {
        System.out.println(body);
    }
}
