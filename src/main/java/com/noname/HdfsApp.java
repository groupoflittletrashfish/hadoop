package com.noname;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * Created by Administrator on 2020/1/17.
 * HDFS java API 操作
 *
 * hadoop 读写流程详解
 * 写：
 *  1. 客户端告知NameNode,副本系数和拆分大小（即：默认以128M大小作为拆分，每个Block保存3个）
 *  2. NameNode 经过计算告知客户端目前有哪些节点空闲，比如说：dataNode1,dataNode2,dataNode3
 *  3. NameNode 将数据拆分成Block发送给dataNode1,dataNode1进行存储
 *  4. 以默认3个副本举例，dataNode1在存储完数据后，将会对下一个节点发起数据传输，如dataNode2,以此类推，直到满足副本个数
 *  5. 一旦数据都传输完毕后，各个dataNode都会通知NameNode
 *  6. 重复以上操作直到整个文件都发送完毕，nameNode将会关闭流，并且汇总block信息，即每个block存放在哪个dataNode中
 *
 * 读:
 *  1. 客户端告知NameNode文件名
 *  2. nameNode 告知客户端文件的每个Block所在的DataNode（有序）
 *  3. 客户端根据顺序从指定的dataNode处下载
 *
 *
 *
 */
public class HdfsApp {

    //此处有个大坑，若hadoop中core-site.xml该文件中的配置填写的是hdfs://localhost:8020，由于ip写的是localhost所以是无法识别的，
    //所以需要将localhost更改为指定的ip地址，重启hadoop才可以生效
    private static final String HDFS_PATH = "hdfs://192.168.222.128:8020";

    private FileSystem fileSystem = null;
    private Configuration configuration = null;


    /**
     * 初始化数据
     *
     * @throws URISyntaxException
     * @throws IOException
     */
    @Before
    public void setUp() throws URISyntaxException, IOException, InterruptedException {
        configuration = new Configuration();
        //创建文件夹，最后一个参数是linux的用户名
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
    }


    /**
     * 创建文件夹
     *
     * @throws IOException
     */
    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/hadoop/test"));
        System.out.println("创建hadoop文件夹成功...");
    }

    /**
     * 创建文本并写入数据
     *
     * @throws IOException
     */
    @Test
    public void create() throws IOException {
        FSDataOutputStream out = fileSystem.create(new Path("/hadoop/test/a.txt"));
        out.write("学习使我快乐".getBytes("UTF-8"));
        out.flush();
        out.close();
    }


    /**
     * 读取文件中的内容
     *
     * @throws IOException
     */
    @Test
    public void cat() throws IOException {
        FSDataInputStream in = fileSystem.open(new Path("/hadoop/test/a.txt"));
        //该工具类用的是Hadoop包下的，用于输出到控制台
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }

    /**
     * 重命名
     *
     * @throws IOException
     */
    @Test
    public void rename() throws IOException {
        boolean rename = fileSystem.rename(new Path("/hadoop/test/a.txt"), new Path("/hadoop/test/b.txt"));
        System.out.println("重命名是否成功：" + rename);
    }

    /**
     * 将本地文件复制到hadoop文件服务器中
     *
     * @throws IOException
     */
    @Test
    public void copyFromLocal() throws IOException {
        fileSystem.copyFromLocalFile(new Path("C:\\Users\\Administrator\\Desktop\\新建文本文档.txt"), new Path("/hadoop/test/c.txt"));
    }


    /**
     * 带进度条的文件上传，与copyFromLocalFile不同的是，需要使用流的方式实现上传
     *
     * @throws IOException
     */
    @Test
    public void copyFromLocalWithProgress() throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(
                new File("C:\\Users\\Administrator\\Downloads\\postgresql-10.11-2-windows-x64-binaries.zip")));
        FSDataOutputStream out = fileSystem.create(new Path("/hadoop/test/progresql.zip"), new Progressable() {
            @Override
            public void progress() {
                //带进度的提示信息
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096);
    }


    /**
     * 从hdfs服务器中下载文件到本地，
     * copyToLocalFile该方法如果只传两个Path参数将可能出现空指针异常，需要使用false,path,path,true的方法
     *
     * @throws IOException
     */
    @Test
    public void copy2Local() throws IOException {
        Path local = new Path("C:\\Users\\Administrator\\Desktop\\a.txt");
        Path hdfsPath = new Path("/hadoop/test/b.txt");
        fileSystem.copyToLocalFile(false, hdfsPath, local, true);
    }


    /**
     * 获取文件列表信息（文件夹/文件）,
     * 副本系数：若通过API创建的文本，则默认的副本系数是3，若通过hadoop fs 指令创建的文本，会根据hadoop的配置文件中
     * 配置的副本系数来创建，即不同的创建方式默认的副本系数不同
     *
     * @throws IOException
     */
    @Test
    public void listFiles() throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hadoop/test/"));
        for (FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            //获取副本个数
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println("是否文件夹:" + isDir);
            System.out.println("副本个数:" + replication);
            System.out.println("文件大小:" + len);
            System.out.println("文件全路径:" + path);
            System.out.println("-----------------------------------");
        }
    }


    /**
     * 删除文件
     *
     * @throws IOException
     */
    @Test
    public void delete() throws IOException {
        //true 代表是否递归删除
        boolean delete = fileSystem.delete(new Path("/hadoop/test/b.txt"), true);
        System.out.println("删除文件成功与否:" + delete);
    }


    /**
     * 释放资源
     */
    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("释放资源...");
    }
}
