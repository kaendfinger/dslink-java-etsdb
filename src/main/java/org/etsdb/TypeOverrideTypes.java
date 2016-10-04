package org.etsdb;

import java.util.LinkedHashSet;
import java.util.Set;

public enum TypeOverrideTypes {
    MAP("map"),
    NUMBER("number"),
    STRING("string"),
    DYNAMIC("dynamic"),
    BOOL("bool"),
    ENUM("enum"),
    BINARY("binary"),
    ARRAY("array"),
    NONE("none");

    private final String name;

    TypeOverrideTypes(String name) {
        this.name = name;
    }

    public static Set<String> buildEnums() {
        Set<String> enums = new LinkedHashSet<>();
        for (TypeOverrideTypes t : TypeOverrideTypes.values()) {
            enums.add(t.getName());
        }
        return enums;
    }

    public String getName() {
        return name;
    }

    public static TypeOverrideTypes fromName(String typeAsString) {
        switch (typeAsString) {
            case "map":
                return MAP;
            case "number":
                return NUMBER;
            case "string":
                return STRING;
            case "dynamic":
                return DYNAMIC;
            case "bool":
                return BOOL;
            case "enum":
                return ENUM;
            case "binary":
                return BINARY;
            case "array":
                return ARRAY;
            case "none":
                return NONE;
            default:
                return NONE;
        }
    }
}
