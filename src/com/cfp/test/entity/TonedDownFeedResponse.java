package com.cfp.test.entity;

import java.util.Map;

public class TonedDownFeedResponse {
    private Map<String, String> header;
    private String diagnosticString;
//    private boolean useEtagAsContinuation;
//    private boolean nochanges;

    public Map<String, String> getHeader() {
        return header;
    }


    public String getDiagnosticString() {
        return diagnosticString;
    }

//    public boolean isUseEtagAsContinuation() {
//        return useEtagAsContinuation;
//    }
//
//    public boolean isNochanges() {
//        return nochanges;
//    }

    public void setHeader(Map<String, String> header) {
        this.header = header;
    }

    public void setDiagnosticString(String diagnosticString) {
        this.diagnosticString = diagnosticString;
    }

//    public void setUseEtagAsContinuation(boolean useEtagAsContinuation) {
//        this.useEtagAsContinuation = useEtagAsContinuation;
//    }
//
//    public void setNochanges(boolean nochanges) {
//        this.nochanges = nochanges;
//    }
}
