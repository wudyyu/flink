package com.flink.wudy.demo.keyBy.models;

import java.util.Objects;

public class ProductInputModel {

    /**
     * 商品id
     */
    private String productId;


    /**
     * 商品销售额
     */
    private Integer income;

    public ProductInputModel(String productId, Integer income) {
        this.productId = productId;
        this.income = income;
    }

    public ProductInputModel() {
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Integer getIncome() {
        return income;
    }

    public void setIncome(Integer income) {
        this.income = income;
    }

    @Override
    public String toString() {
        return "ProductInputModel{" +
                "productId='" + productId + '\'' +
                ", income=" + income +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProductInputModel)) return false;
        ProductInputModel that = (ProductInputModel) o;
        return Objects.equals(productId, that.productId) && Objects.equals(income, that.income);
    }

    @Override
    public int hashCode() {
        return Objects.hash(productId, income);
    }
}
