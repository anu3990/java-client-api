package com.marklogic.client.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.expression.ValidateDocErrorDispositionDef;

public class ValidateDocErrorDispositionDefImpl implements ValidateDocErrorDispositionDef, BaseTypeImpl.BaseArgImpl {
    private String logLevel;
    private int logSize;

    public ValidateDocErrorDispositionDefImpl() {

    }
    @Override
    public StringBuilder exportAst(StringBuilder strb) {
        ObjectNode node = new ObjectMapper().createObjectNode();
        if(logLevel != null)
            node.put("logLevel", logLevel);
        node.put("logSize", logSize);
        return strb.append(node.toString());
    }

    @Override
    public ValidateDocErrorDispositionDef withLogSize(int logSize) {
        this.logSize = logSize;
        return this;
    }

    @Override
    public ValidateDocErrorDispositionDef withLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return this;
    }
}
