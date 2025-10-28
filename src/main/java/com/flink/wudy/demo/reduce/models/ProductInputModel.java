package com.flink.wudy.demo.reduce.models;

import java.math.BigDecimal;
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

    /**
     * 商品销量
     */
    private Integer count;

    /**
     * 商品平均销售额
     */
    private BigDecimal avgPrice;

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

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public BigDecimal getAvgPrice() {
        return avgPrice;
    }

    public void setAvgPrice(BigDecimal avgPrice) {
        this.avgPrice = avgPrice;
    }

    public ProductInputModel() {
    }

    public ProductInputModel(String productId, Integer income, Integer count, BigDecimal avgPrice) {
        this.productId = productId;
        this.income = income;
        this.count = count;
        this.avgPrice = avgPrice;
    }

    @Override
    public String toString() {
        return "ProductInputModel{" +
                "productId='" + productId + '\'' +
                ", income=" + income +
                ", count=" + count +
                ", avgPrice=" + avgPrice +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProductInputModel)) return false;
        ProductInputModel that = (ProductInputModel) o;
        return Objects.equals(productId, that.productId) && Objects.equals(income, that.income) && Objects.equals(count, that.count) && Objects.equals(avgPrice, that.avgPrice);
    }

    @Override
    public int hashCode() {
        return Objects.hash(productId, income, count, avgPrice);
    }
}
