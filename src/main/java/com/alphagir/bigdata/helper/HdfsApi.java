package com.alphagir.bigdata.helper;

import com.alphagir.bigdata.exception.ReportBadRequestException;
import com.alphagir.bigdata.model.HdfsFileStatus;
import com.alphagir.bigdata.model.HdfsPermissionEnum;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import javax.activation.MimetypesFileTypeMap;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.alphagir.bigdata.model.HdfsPermissionEnum.*;

@Slf4j
public class HdfsApi {

    private String uri;
    private UserGroupInformation ugi;
    private FileSystem fs;
    private Configuration conf;

    public HdfsApi(final String uri, String user) throws IOException, InterruptedException {
        this.conf = new Configuration();
        conf.set("fs.defaultFS", uri);
        conf.set("hadoop.root.logger", "ERROR,console");
        this.uri = uri;
        UserGroupInformation.setConfiguration(conf);

        if (StringUtils.isNotBlank(user)) {
            // 创建远程用户
            this.ugi = UserGroupInformation.createRemoteUser(user);
        } else {
            // 获得当前用户
            this.ugi = UserGroupInformation.getCurrentUser();
        }

        initializeFileSystem();
    }

    private void initializeFileSystem() throws IOException, InterruptedException {
        // 放在doAs里面执行action，并获得fs实例
        this.fs = execute(() -> FileSystem.get(conf));

        // 如果未指定HDFS文件系统的uri，则默认为本地系统，替换file为本地C盘
        if (StringUtils.isBlank(uri)) {
            this.uri = conf.get("fs.default.name");
            if (uri.equals("file:///")) {
                this.uri = "C:";
            }
        }

    }

    public synchronized FsStatus getHdfsStatus() throws Exception {
        return execute(() -> {
            FsStatus status = fs.getStatus();
            log.info("容量：" + getByteToSize(status.getCapacity()));
            log.info("已用：" + getByteToSize(status.getUsed()));
            log.info("剩余：" + getByteToSize(status.getRemaining()));
            return status;
        });
    }

    public Boolean mkdir(final String path) throws IOException, InterruptedException {
        return execute(() -> {
            Path dPath = new Path(uri + "/" + path);
            return fs.mkdirs(dPath);
        });

    }

    public FSDataOutputStream createFile(final String path, final boolean overwrite)
            throws IOException, InterruptedException {
        return execute(() -> fs.create(new Path(uri + "/" + path), overwrite));
    }

    public FSDataOutputStream appendFile(final String path) throws IOException, InterruptedException {
        return execute(() -> fs.append(new Path(uri + "/" + path)));
    }

    public boolean rmdir(final String path, boolean recursive, boolean skiptrash)
            throws IOException, InterruptedException {
        return execute(() -> {
            try {
                Path dPath;
                String destPath = "";
                if (StringUtils.isNotBlank(uri)) {
                    destPath = uri + "/" + path;
                    dPath = new Path(destPath);
                } else {
                    destPath = path;
                    dPath = new Path(path);
                }

                // 如果不跳过回收站，则将删除的对象放入回收站
                if (!skiptrash) {
                    moveToTrash(destPath);
                    log.info(destPath + " 移动到回收站成功！");
                    return true;
                } else {
                    // 是否删除文件目录的时候，采用递归删除文件
                    log.info(destPath + " 删除成功！");
                    return fs.delete(dPath, recursive);
                }
            } catch (IllegalArgumentException | IOException | InterruptedException e) {
                log.error(e.getClass() + "," + e.getMessage());
            }
            return false;
        });
    }

    public List<HdfsFileStatus> getFileList(final String path, PathFilter pathFilter)
            throws IOException, InterruptedException {
        return execute(() -> {
            ObjectMapper mapper = new ObjectMapper();
            List<HdfsFileStatus> models = new ArrayList<>();
            try {
                Path dPath;
                if (StringUtils.isNotBlank(uri)) {
                    dPath = new Path(uri + "/" + path);
                } else {
                    dPath = new Path(path);
                }
                FileStatus[] status;
                if (pathFilter != null) {
                    // 根据filter列出目录内容
                    status = fs.listStatus(dPath, pathFilter);
                } else {
                    // 列出目录内容
                    status = fs.listStatus(dPath);
                }
                for (FileStatus fileStatus : status) {
                    models.add(fileStatusToModel(fileStatus));
                }
//                log.info("文件列表状态：" + mapper.writeValueAsString(models));
            } catch (IllegalArgumentException | IOException e) {
                e.printStackTrace();
            }
            return models;

        });

    }

    public HdfsFileStatus getOneFileStatus(final Path filePath) throws IOException {
        FileStatus status = fs.getFileStatus(filePath);
        return this.fileStatusToModel(status);
    }

    private HdfsFileStatus fileStatusToModel(FileStatus status) {

        HdfsFileStatus hdfsFileStatus = new HdfsFileStatus();
        hdfsFileStatus.setPath(Path.getPathWithoutSchemeAndAuthority(status.getPath()).toString());
        hdfsFileStatus.setReplication(status.getReplication());
        hdfsFileStatus.setIsDirectory(status.isDirectory());
        hdfsFileStatus.setLen(status.getLen());
        // 文件夹大小显示为 --
        if (status.isDirectory()) {
            hdfsFileStatus.setSize("--");
        } else {
            hdfsFileStatus.setSize(getByteToSize(status.getLen()));
        }
        hdfsFileStatus.setOwner(status.getOwner());
        hdfsFileStatus.setGroup(status.getGroup());
        hdfsFileStatus.setPermission(permissionToString(status.getPermission()));
        hdfsFileStatus.setCreatedAt(LocalDateTime.ofInstant(Instant.ofEpochMilli(status.getAccessTime()), ZoneId.systemDefault()));
        hdfsFileStatus.setUpdatedAt(LocalDateTime.ofInstant(Instant.ofEpochMilli(status.getModificationTime()), ZoneId.systemDefault()));
        hdfsFileStatus.setBlockSize(status.getBlockSize());
        hdfsFileStatus.setReadAccess(checkAccessPermissions(status, FsAction.READ, ugi));
        hdfsFileStatus.setWriteAccess(checkAccessPermissions(status, FsAction.READ, ugi));
        hdfsFileStatus.setExecuteAccess(checkAccessPermissions(status, FsAction.READ, ugi));

        return hdfsFileStatus;
    }

    public String permissionToString(FsPermission p) {
        return (p == null) ? "default"
                : "-" + p.getUserAction().SYMBOL + p.getGroupAction().SYMBOL + p.getOtherAction().SYMBOL;
    }

    public boolean checkAccessPermissions(FileStatus stat, FsAction mode, UserGroupInformation ugi) {

        FsPermission perm = stat.getPermission();
        String user = ugi.getShortUserName();
        List<String> groups = Arrays.asList(ugi.getGroupNames());
        if (user.equals(stat.getOwner())) {
            return perm.getUserAction().implies(mode);
        } else if (groups.contains(stat.getGroup())) {
            return perm.getGroupAction().implies(mode);
        } else {
            return perm.getOtherAction().implies(mode);
        }
    }

    public void uploadFile(final String srcFile, final String destPath, boolean delSrc, boolean overwrite)
            throws IOException, InterruptedException {
        execute((PrivilegedExceptionAction<Void>) () -> {

            // 源文件路径
            Path srcPath = new Path(srcFile);
            // 目标文件要存放的目录如果不存在，则创建
            existDir(destPath, true);
            // 目标文件Path
            Path dPath;
            if (StringUtils.isNotBlank(uri)) {
                dPath = new Path(uri + "/" + destPath);
            } else {
                // 否者 默认上传到根目录下
                dPath = new Path(uri + "/");
            }

            // 实现文件上传
            try {
                fs.copyFromLocalFile(delSrc, overwrite, srcPath, dPath);
                log.info("文件：" + srcPath + ",上传成功！");
            } catch (IOException e) {
                log.error(e.getClass() + "," + e.getMessage());
            }
            return null;
        });

    }

    public HdfsFileStatus uploadFile(InputStream in, final String destPath) throws IOException, InterruptedException {
        return execute(() -> {
            // 目标文件Path
            Path dPath;
            if (StringUtils.isNotBlank(uri)) {
                dPath = new Path(uri + "/" + destPath);
            } else {
                // 否者 默认上传到根目录下
                dPath = new Path(uri + "/");
            }

            OutputStream os = fs.create(dPath);
            /*
             * in ：输入字节流（从要上传的文件中读取）
             * out：输出字节流（字节输出到目标文件）
             * 2048：每次写入2048
             * true：不管成功与否，最后都关闭stream资源
             */
            org.apache.hadoop.io.IOUtils.copyBytes(in, os, 2048, true);
            log.info(dPath + " 写入成功！");
            return this.getOneFileStatus(dPath);
        });
    }

    public void downloadFile(final String srcFile, final String destPath) throws IOException, InterruptedException {

        execute((PrivilegedExceptionAction<Void>) () -> {
            // 源路径
            Path sPath;
            if (StringUtils.isNotBlank(uri)) {
                sPath = new Path(uri + "/" + srcFile);
            } else {
                sPath = new Path(srcFile);
            }

            /*
             * 本地路径或者Linux下路径
             */
            Path dstPath = new Path(destPath);
            try {
                fs.copyToLocalFile(sPath, dstPath);
                log.info("文件下载至：" + destPath + sPath.getName());
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    public void downloadFile(final String srcFile, HttpServletResponse response) throws IOException, InterruptedException {

        execute((PrivilegedExceptionAction<Void>) () -> {
            // 源路径
            Path sPath;
            if (StringUtils.isNotBlank(uri)) {
                sPath = new Path(uri + "/" + srcFile);
            } else {
                sPath = new Path(srcFile);
            }


            String fileName = srcFile.substring(srcFile.lastIndexOf("/") + 1);
            log.debug(fileName);

            response.setContentType(new MimetypesFileTypeMap().getContentType(new File(fileName)));
            response.setHeader("Content-Disposition",
                    "attachment;filename=" + URLEncoder.encode(fileName, StandardCharsets.UTF_8));

            try {
                FSDataInputStream is = fs.open(sPath);
                OutputStream out = response.getOutputStream();
                long position;
                while (true) {
                    byte[] data = is.readNBytes(1024);
                    if (data == null || data.length <= 0) {
                        break;
                    }
                    out.write(data);
                    position = is.getPos();
                    is.seek(position);
                }
                out.flush();
                is.close();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    public BlockLocation[] getFileBlockLocations(final String filePath) {

        Path path;
        if (StringUtils.isNotBlank(uri)) {
            path = new Path(uri + "/" + filePath);
        } else {
            path = new Path(filePath);
        }
        // 文件块位置列表
        BlockLocation[] blkLocations = new BlockLocation[0];
        try {
            // 获取文件目录
            FileStatus filestatus = fs.getFileStatus(path);
            // 获取文件块位置列表
            blkLocations = fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());
            for (BlockLocation blockLocation : blkLocations) {
                long length = blockLocation.getLength();
                log.info("文件块的长度[" + length + "/1024 = 文件的大小" + (length / 1024d) + "kb]：" + length);
                log.info("文件块的偏移量：" + blockLocation.getOffset());
                List<String> nodes = new ArrayList<>();
                Collections.addAll(nodes, blockLocation.getHosts());
                log.info("文件块存储的主机（DataNode）列表（主机名）：" + nodes);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return blkLocations;
    }

    public boolean rename(final String srcPath, final String dstPath) throws Exception {
        return execute(() -> {
            boolean flag = false;
            try {

                Path sPath;
                Path dPath;

                if (StringUtils.isNotBlank(uri)) {
                    sPath = new Path(uri + "/" + srcPath);
                    dPath = new Path(uri + "/" + dstPath);
                } else {
                    sPath = new Path(srcPath);
                    dPath = new Path(dstPath);
                }

                if (sPath.getName().equals(dPath.getName())) {
                    flag = true;
                } else {
                    flag = fs.rename(sPath, dPath);
                }

                log.info(srcPath + " 重命名到 " + dstPath + " 成功！");
            } catch (IOException e) {
                log.error(srcPath + " 重命名到 " + dstPath + " 失败：" + e.getMessage());
            }

            return flag;
        });

    }

    public boolean exists(final String srcPath) throws IOException, InterruptedException {
        return execute(() -> {
            Path sPath;
            if (StringUtils.isNotBlank(uri)) {
                sPath = new Path(uri + "/" + srcPath);
            } else {
                sPath = new Path(srcPath);
            }
            return fs.exists(sPath);
        });
    }

    public boolean existDir(final String dirPath, boolean create) throws IOException, InterruptedException {
        return execute(() -> {
            boolean flag = false;
            Path dPath;
            if (StringUtils.isEmpty(dirPath)) {
                return flag;
            } else {
                dPath = new Path(uri + "/" + dirPath);
            }
            try {
                if (create) {
                    if (!fs.exists(dPath)) {
                        fs.mkdirs(dPath);
                    }
                }
                // 如果是目录，返回true
                if (fs.isDirectory(dPath)) {
                    flag = true;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return flag;
        });

    }

    public FSDataInputStream open(final String path) throws IOException, InterruptedException {
        return execute(() -> fs.open(new Path(uri + "/" + path)));
    }

    public boolean chmod(final String path, final String permissions) throws IOException, InterruptedException {
        return execute(() -> {
            try {
                String nPath = uri + "/" + path;
                FsPermission fp = FsPermission.valueOf(permissions);
                fs.setPermission(new Path(nPath), fp);
                log.info("修改文件/目录：" + nPath + "的权限为" + fp.toString() + "，成功！");
            } catch (Exception ex) {
                return false;
            }
            return true;
        });
    }

    public void copy(final String src, final String dest) throws Exception {

        boolean result = execute(() -> {
            // 是否删除源文件 == false，不删除源文件
            return FileUtil.copy(fs, new Path(uri + "/" + src), fs, new Path(uri + "/" + dest), false, conf);
        });

        if (!result) {
            throw new Exception("HDFS010 无法将文件从：" + src + " 复制到：" + dest);
        }
    }

    public void move(final String src, final String dest) throws Exception {
        boolean result = execute(() -> {
            /**
             * 是否删除源文件 == true，删除源文件 copy原理: 1.先复制字节 2.然后递归删除源文件或目录
             */
            Path sPath;
            Path dPath;
            String srcPath;
            if (StringUtils.isNotBlank(uri)) {
                srcPath = uri + "/" + src;
                sPath = new Path(srcPath);
                dPath = new Path(uri + "/" + dest);
            } else {
                srcPath = src;
                sPath = new Path(srcPath);
                dPath = new Path(dest);
            }
            if (!existFile(src)) {
                log.info(srcPath + "不存在，本次移动操作终止");
                return false;
            }

            // Move to same path, so can not delete the srcPath
            int index = src.lastIndexOf("/");
            Boolean deleteSource = true;
            if (src.substring(0, index).equals(dest)) {
                deleteSource = false;
            }
            boolean copy = FileUtil.copy(fs, sPath, fs, dPath, deleteSource, conf);
            return copy;
        });

        if (!result) {
            throw new Exception("HDFS010 无法将文件从 " + src + " 复制到 " + dest);
        }

    }

    public boolean existFile(final String filePath) throws IOException, InterruptedException {
        return execute(() -> {
            boolean flag = false;
            if (StringUtils.isEmpty(filePath)) {
                return flag;
            }
            try {
                Path path;
                if (StringUtils.isNotBlank(uri)) {
                    path = new Path(uri + "/" + filePath);
                } else {
                    path = new Path(filePath);
                }
                // 如果文件存在，返回true
                if (fs.exists(path)) {
                    flag = true;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return flag;
        });

    }

    public Path getHomeDir() throws Exception {
        return execute(() -> fs.getHomeDirectory());
    }


    public boolean trashEnabled() throws Exception {
        return execute(() -> {
            Trash trash = new Trash(fs, conf);
            log.info("Whether the recycle bin is enabled: " + trash.isEnabled());
            return trash.isEnabled();
        });
    }


    public Path getTrashDir() throws Exception {
        return execute(() -> {
            TrashPolicy trashPolicy = TrashPolicy.getInstance(conf, fs, fs.getHomeDirectory());
            return trashPolicy.getCurrentTrashDir().getParent();
        });
    }


    public String getTrashDirPath() throws Exception {
        Path trashDir = getTrashDir();
        // 返回回收站目录URI的原始路径组件 == 字符串形式的路径
        return trashDir.toUri().getRawPath();

    }


    public FileSystem getFs() {
        return fs;
    }


    public void setFs(FileSystem fs) {
        this.fs = fs;
    }


    public String getTrashDirPath(final String filePath) throws Exception {
        String trashDirPath = getTrashDirPath();
        Path path = new Path(filePath);
        trashDirPath = trashDirPath + "/" + path.getName();
        return trashDirPath;
    }


    public boolean moveToTrash(final String path) throws IOException, InterruptedException {
        return execute(new PrivilegedExceptionAction<Boolean>() {
            public Boolean run() throws IOException, InterruptedException {
                /*
                 * 注意这里有个"bug"，如果使用方法Trash.moveToAppropriateTrash(fs, new
                 * Path(path), conf);
                 * 会报异常，跟进去发下，fs传进去后，只是做了陪衬，在其方法内部会重新拿到FileSystem
                 * 因此，这里会造成用户的丢失，比如，最开始设置的root用户，进去后就成了Administrator用户
                 */

                Trash trash = new Trash(fs, conf);
                return trash.moveToTrash(new Path(path));
            }
        });
    }


    public boolean restoreFromTrash(final String srcPath, final String destPath)
            throws IOException, InterruptedException {
        return execute(new PrivilegedExceptionAction<Boolean>() {
            @Override
            public Boolean run() throws Exception {
                /*
                 * 把源文件从回收站里面移除来
                 */
                move(srcPath, destPath);
                return true;
            }

        });

    }


    public boolean emptyTrash() throws Exception {
        return execute(() -> {

            // 第一种方法：使用递归删除目录，暴力清空
            // rmdir(getTrashDirPath(), true, true);

            // 第二种方法：使用expunge方法，删除掉旧的检查点
            Trash tr = new Trash(fs, conf);
            tr.expunge();
            log.info("垃圾清理完成！");
            return true;
        });
    }


    public void putStringToFile(final String filePath, final String content) throws ReportBadRequestException {
        try {
            execute((PrivilegedExceptionAction<Void>) () -> {
                // 创建一个文件，并拿到文件的FS数据输出流，便于写入字节
                final FSDataOutputStream stream = createFile(filePath, true);
                stream.write(content.getBytes());
                stream.close();
                return null;
            }, true);
        } catch (IOException e) {
            throw new ReportBadRequestException("HDFS020 Could not write file " + filePath, e);
        } catch (InterruptedException e) {
            throw new ReportBadRequestException("HDFS021 Could not write file " + filePath, e);
        }
    }


    public void appendStringToFile(final String filePath, final String content) throws ReportBadRequestException {
        try {
            execute((PrivilegedExceptionAction<Void>) () -> {
                // 创建一个文件，并拿到文件的FS数据输出流，便于写入字节
                final FSDataOutputStream stream = appendFile(filePath);
                stream.write(content.getBytes());
                stream.close();
                return null;
            }, true);
        } catch (IOException e) {
            throw new ReportBadRequestException("HDFS020 Could not append file " + filePath, e);
        } catch (InterruptedException e) {
            throw new ReportBadRequestException("HDFS021 Could not append file " + filePath, e);
        }
    }

    public String readFileToString(final String filePath) throws ReportBadRequestException {
        FSDataInputStream stream;
        try {
            // 打开一个文件，获得FS数据输入流，便于读取输出
            stream = open(filePath);
            return IOUtils.toString(stream);
        } catch (IOException e) {
            throw new ReportBadRequestException("HDFS060 Could not read file " + filePath, e);
        } catch (InterruptedException e) {
            throw new ReportBadRequestException("HDFS061 Could not read file " + filePath, e);
        }

    }

    private <T> T execute(PrivilegedExceptionAction<T> action) throws IOException, InterruptedException {
        return execute(action, false);
    }

    private <T> T execute(PrivilegedExceptionAction<T> action, boolean alwaysRetry)
            throws IOException, InterruptedException {

        T result = null;

        /*
         * 由于HDFS-1058，这里采用了重试策略。HDFS可以随机抛出异常 IOException关于从DN中检索块(如果并发读写)
         * 在特定文件上执行(参见HDFS-1058的详细信息)。
         */
        int tryNumber = 0;
        boolean succeeded = false;
        do {
            tryNumber += 1;
            try {
                // doAs中执行的操作都是以proxyUser用户的身份执行
                result = ugi.doAs(action);
                succeeded = true;
            } catch (IOException ex) {
                if (!Strings.isNullOrEmpty(ex.getMessage()) && !ex.getMessage().contains("无法获取块的长度：")) {
                    throw ex;
                }

                // 尝试超过>=3次，抛出异常，do while 退出
                if (tryNumber >= 3) {
                    throw ex;
                }
                log.error("HDFS抛出'IOException:无法获得块长度'的异常. " + "再次尝试... 尝试 #" + (tryNumber + 1));
                log.error("再次尝试: " + ex.getMessage(), ex);
                Thread.sleep(1000); // 1s后再试
            }
        } while (!succeeded);
        return result;
    }

    private String getByteToSize(long size) {

        StringBuilder bytes = new StringBuilder();
        // 保留两位有效数字
        DecimalFormat format = new DecimalFormat("###.00");
        if (size >= 1024 * 1024 * 1024) {
            double i = (size / (1024.0 * 1024.0 * 1024.0));
            bytes.append(format.format(i)).append("GiB");
        } else if (size >= 1024 * 1024) {
            double i = (size / (1024.0 * 1024.0));
            bytes.append(format.format(i)).append("MiB");
        } else if (size >= 1024) {
            double i = (size / (1024.0));
            bytes.append(format.format(i)).append("KiB");
        } else {
            if (size <= 0) {
                bytes.append("--");
            } else {
                bytes.append((int) size).append("B");
            }
        }
        return bytes.toString();
    }

    public void close() throws IOException {
        fs.close();
    }


//    public String getAcl(String path) throws IOException {
//        String nPath = uri + "/" + path;
//        AclStatus fsPermission = fs.getAclStatus(new Path(nPath));
//        return fsPermission.toString();
//    }

//    public FileStatus getFileStatus(String path) throws IOException {
//        String nPath = uri + "/" + path;
//        Path paths = new Path(nPath);
//        return fs.getFileStatus(paths);
//    }

    public FileStatus[] getListStatus(String path) throws IOException {
        String nPath = uri + "/" + path;
        Path paths = new Path(nPath);
        return fs.listStatus(paths);
    }

    private FsAction getFsAction(HdfsPermissionEnum action) {
        FsAction fsAction;
//        PermissionHDFS action = PermissionHDFS.valueOf(key);
        if (action.equals(ALL)) {
            fsAction = FsAction.ALL;
        } else if (action.equals(READ)) {
            fsAction = FsAction.READ;
        } else if (action.equals(NONE)) {
            fsAction = FsAction.NONE;
        } else if (action.equals(WRITE)) {
            fsAction = FsAction.WRITE;
        } else if (action.equals(EXECUTE)) {
            fsAction = FsAction.EXECUTE;
        } else if (action.equals(READ_EXECUTE)) {
            fsAction = FsAction.READ_EXECUTE;
        } else if (action.equals(READ_WRITE)) {
            fsAction = FsAction.READ_WRITE;
        } else if (action.equals(WRITE_EXECUTE)) {
            fsAction = FsAction.WRITE_EXECUTE;
        } else {
            fsAction = FsAction.NONE;
        }
        return fsAction;
    }

    public boolean createDir(String path) throws IOException {
        Path paths = new Path(uri + "/" + path);
        FsAction u = FsAction.ALL;
        FsAction g = FsAction.ALL;
        FsAction o = FsAction.ALL;
        return fs.mkdirs(paths, new FsPermission(u, g, o, false));
//        return fs.mkdirs(paths);
    }

    public boolean isDir(String path) throws IOException {
        Path paths = new Path(uri + "/" + path);
        return fs.exists(paths);
    }

    public FsPermission updatePermission(String path, HdfsPermissionEnum owner, HdfsPermissionEnum group, HdfsPermissionEnum other, boolean sb) throws IOException {
        Path paths = new Path(uri + "/" + path);
        FsPermission permission = new FsPermission(this.getFsAction(owner), this.getFsAction(group), this.getFsAction(other), sb);
        fs.setPermission(paths, permission);
        FileStatus fileStatus = fs.getFileStatus(paths);
        return fileStatus.getPermission();
    }

    public FsPermission updateOwner(String path, String username, String group) throws IOException {
        Path paths = new Path(uri + "/" + path);
        fs.setOwner(paths, username, group);
        FileStatus fileStatus = fs.getFileStatus(paths);
        return fileStatus.getPermission();
    }

//    public void createAcl(String path,String name) throws IOException {
//        Path paths = new Path(uri + "/" + path);
//        List<AclEntry> aclEntryList = new ArrayList<>();
//        aclEntryList.add(new AclEntry.Builder().setType(USER).setName(name).setPermission(FsAction.ALL).setScope(AclEntryScope.ACCESS).build());
//        aclEntryList.add(new AclEntry.Builder().setType(GROUP).setName("GROUP" + name).setPermission(FsAction.NONE).setScope(AclEntryScope.DEFAULT).build());
//        aclEntryList.add(new AclEntry.Builder().setType(OTHER).setName("OTHER" + name).setPermission(FsAction.NONE).setScope(AclEntryScope.DEFAULT).build());
//        fs.setAcl(paths,aclEntryList);
//    }

}
