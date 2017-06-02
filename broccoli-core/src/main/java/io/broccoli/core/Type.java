package io.broccoli.core;

/**
 * @author nicola
 * @since 02/06/2017
 */
public enum Type {

    INTEGER(Long.class),
    STRING(String.class);

    private Class<?> javaType;

    Type(Class<?> javaType) {
        this.javaType = javaType;
    }

    public Class<?> getJavaType() {
        return javaType;
    }

    public static Type fromJavaType(Class<?> type) {
        if (String.class.isAssignableFrom(type)) {
            return Type.STRING;
        } else if (Long.class.isAssignableFrom(type) || Integer.class.isAssignableFrom(type)) {
            return Type.INTEGER;
        } else {
            throw new IllegalArgumentException("Unsupported java type: " + type);
        }
    }
}
