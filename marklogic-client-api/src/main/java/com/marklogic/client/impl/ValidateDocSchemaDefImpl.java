package com.marklogic.client.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.expression.ValidateDocSchemaDefinition;

import java.util.Map;

public class ValidateDocSchemaDefImpl implements ValidateDocSchemaDefinition, BaseTypeImpl.BaseArgImpl {
    private String kind;
    private String schema;
    private String mode;
    private String schemaUri;

    @Override
    public StringBuilder exportAst(StringBuilder strb) {
        ObjectNode node = new ObjectMapper().createObjectNode();
        if(kind != null)
            node.put("kind", kind);
        if(schema != null)
            node.put("schema", schema);
        if(mode != null)
            node.put("mode", mode);
        if(schemaUri != null)
            node.put("schemaUri", schemaUri);
        return strb.append(node.toString());
    }

    @Override
    public ValidateDocSchemaDefinition withKind(String kind) {
        this.kind = kind;
        return this;
    }

    @Override
    public ValidateDocSchemaDefinition withSchema(Map<Object, Object> mapping) {
        ObjectNode descriptor = new ObjectMapper().createObjectNode();
        mapping.keySet().forEach(key -> descriptor.put(key.toString(), mapping.get(key).toString()));
        this.schema = descriptor.toString();
        return this;
    }

    @Override
    public ValidateDocSchemaDefinition withMode(String mode) {
        this.mode = mode;
        return this;
    }

    @Override
    public ValidateDocSchemaDefinition withSchemaUri(String schemaUri) {
        this.schemaUri = schemaUri;
        return this;
    }
}
