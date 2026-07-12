package com.mulegraph.ingestion.api;

import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import com.mulegraph.ingestion.port.TransactionPublisher;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/transactions")
public class TransactionController {

    private final TransactionPublisher publisher;

    public TransactionController(TransactionPublisher publisher) {
        this.publisher = publisher;
    }

    @PostMapping
    public ResponseEntity<TransactionResponse> ingestTransaction(@Valid @RequestBody TransactionRequest request) {
        if (request.sourceAccountId().equals(request.destinationAccountId())) {
            throw new IllegalArgumentException("Source and destination accounts must be different.");
        }

        UUID eventId = UUID.randomUUID();
        String correlationId = UUID.randomUUID().toString();
        Instant ingestedAt = Instant.now();

        InternalTransactionEvent event = new InternalTransactionEvent(
                eventId,
                request.transactionId(),
                1, // schema_version
                correlationId,
                request.sourceAccountId(),
                request.destinationAccountId(),
                request.amountMinor(),
                request.currency(),
                request.deviceId(),
                request.ipHash(),
                request.occurredAt(),
                ingestedAt
        );

        publisher.publish(event);

        TransactionResponse response = new TransactionResponse(
                eventId,
                request.transactionId(),
                "ACCEPTED"
        );

        return ResponseEntity.accepted().body(response);
    }
}
