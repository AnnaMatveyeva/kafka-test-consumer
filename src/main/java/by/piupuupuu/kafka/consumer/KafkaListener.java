package by.piupuupuu.kafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaListener {

    private final Flux<ReceiverRecord<String, String>> reactiveKafkaReceiver;


    @EventListener(ApplicationStartedEvent.class)
    public void onMessage() {
        reactiveKafkaReceiver
                .doOnNext(r -> log.info(r.value()))
                .doOnError(e -> log.error("KafkaFlux exception", e))
                .subscribe();
    }
}
