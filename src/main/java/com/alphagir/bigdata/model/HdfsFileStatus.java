package com.alphagir.bigdata.model;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class HdfsFileStatus {

    private String path;

    private String format;

    private Short replication;

    private String uuid;

    private Boolean isDirectory;

    private Long len;

    private String size;

    private String owner;

    private String group;

    private String permission;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    private Long blockSize;

    private Boolean readAccess;

    private Boolean writeAccess;

    private Boolean executeAccess;

}
