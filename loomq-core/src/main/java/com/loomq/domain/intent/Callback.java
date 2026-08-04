package com.loomq.domain.intent;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 回调配置
 *
 * @author loomq
 * @since v0.5.0
 */
public class Callback {

    /**
     * 回调 HTTP 地址
     */
    private String url;

    /**
     * HTTP 方法，默认 POST
     */
    private String method;

    /**
     * 自定义请求头
     */
    private Map<String, String> headers;

    /**
     * 回调请求体（JSON 对象或字符串）
     */
    private Object body;

    public Callback() {
        this.method = "POST";
    }

    public Callback(String url) {
        this();
        this.url = url;
    }

    public Callback(String url, String method, Map<String, String> headers, Object body) {
        this.url = url;
        this.method = Objects.requireNonNullElse(method, "POST");
        this.headers = headers;
        this.body = body;
    }

    // ========== Getters / Setters ==========

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public Object getBody() {
        return body;
    }

    public void setBody(Object body) {
        this.body = body;
    }

    /**
     * 创建当前 Callback 的独立副本（I5 边界不变量）。
     *
     * <p>headers 经防御性拷贝（可变副本，容许 null 值）。body（Object 类型）无法泛型深拷贝，
     * 按引用共享--已知限制，使用方不应通过 body 间接变异内核状态。</p>
     *
     * @return 独立副本
     */
    public Callback copy() {
        return new Callback(url, method,
            headers != null ? new HashMap<>(headers) : null, body);
    }

    @Override
    public String toString() {
        return String.format("Callback{url=%s, method=%s}", url, method);
    }
}
