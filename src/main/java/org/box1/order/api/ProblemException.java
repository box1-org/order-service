package org.box1.order.api;

/** A request failure that maps directly to a Problem response. */
public class ProblemException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final int status;
    private final String title;

    public ProblemException(int status, String title, String detail) {
        super(detail);
        this.status = status;
        this.title = title;
    }

    public int status() {
        return status;
    }

    public String title() {
        return title;
    }
}
