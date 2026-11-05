package org.box1.order.domain;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** Fingerprints a create request by value, independent of JSON formatting and field order. */
public final class Fingerprint {
    private Fingerprint() {}

    /** SHA-256 over length-prefixed fields, so adjacent values cannot run together. */
    public static String of(String customerId, String sku, int quantity, String siteId) {
        StringBuilder canonical = new StringBuilder();
        for (String field : new String[] {customerId, sku, Integer.toString(quantity), siteId}) {
            canonical.append(field.length()).append(':').append(field);
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }
}
