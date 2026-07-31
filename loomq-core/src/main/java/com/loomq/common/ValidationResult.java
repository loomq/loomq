package com.loomq.common;

/**
 * 验证结果。
 *
 * @author loomq
 */
public record ValidationResult(boolean valid, String errorMessage) {

    /** 验证通过 */
    public static final ValidationResult VALID = new ValidationResult(true, null);

    /** 创建验证失败结果 */
    public static ValidationResult error(String errorMessage) {
        return new ValidationResult(false, errorMessage);
    }

    public boolean isValid() {
        return valid;
    }

    public boolean isInvalid() {
        return !valid;
    }
}
