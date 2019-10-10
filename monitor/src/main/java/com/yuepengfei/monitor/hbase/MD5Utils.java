package com.suning.flink.common;


import com.jcraft.jsch.jce.MD5;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Utils extends MD5 {
    /*
     * MD5摘要方式
     */
    private static final String MESSAGE_DIGEST_MD5 = "MD5";
    /*
     * 字符集UTF-8
     */
    private static final String CHARSET_UTF8 = "UTF-8";
    /*
     * 字符0
     */
    private static final String STRING_ZERO = "0";

    /**
     * 将input进行md5摘要,并base 16
     * @param input
     * @return
     */
    public static String evaluate(String input) throws UnsupportedEncodingException,NoSuchAlgorithmException {
        // 获得MD5摘要算法的 MessageDigest 对象
        MessageDigest md5Inst = MessageDigest.getInstance(MESSAGE_DIGEST_MD5);
        // 获得密文
        byte[] md5Bytes = md5Inst.digest(input.getBytes(CHARSET_UTF8));
        // 把密文转换成十六进制的字符串形式
        StringBuilder hexValue = new StringBuilder();
        // 字节数组转换为十六进制数
        for (int i = 0; i < md5Bytes.length; i++) {
            int val = ((int) md5Bytes[i]) & 0xff;
            if (val < 16) {
                hexValue.append(STRING_ZERO);
            }
            hexValue.append(Integer.toHexString(val));
        }
        return hexValue.toString();
    }
}