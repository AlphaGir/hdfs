package com.alphagir.bigdata.model;

public enum HdfsPermissionEnum {

    ALL("所有权限", "ALL"),
    READ("只读权限", "READ"),
    NONE("无权限", "NONE"),
    WRITE("写权限", "WRITE"),
    EXECUTE("执行权限", "EXECUTE"),
    READ_EXECUTE("读和执行权限", "READ_EXECUTE"),
    READ_WRITE("读和写权限", "READ_WRITE"),
    WRITE_EXECUTE("执行和写权限", "WRITE_EXECUTE");

    private final String name;
    private final String value;

    HdfsPermissionEnum(String name, String value) {
        this.value = value;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }
}
