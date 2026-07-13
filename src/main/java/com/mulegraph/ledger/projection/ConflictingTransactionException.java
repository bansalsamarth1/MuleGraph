package com.mulegraph.ledger.projection;

public class ConflictingTransactionException extends RuntimeException {
    public ConflictingTransactionException(String message) {
        super(message);
    }
}
