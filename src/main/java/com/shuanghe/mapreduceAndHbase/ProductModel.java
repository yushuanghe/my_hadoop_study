package com.shuanghe.mapreduceAndHbase;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义数据格式
 * Created by yushuanghe on 2017/02/16.
 */
public class ProductModel implements WritableComparable<ProductModel> {

    private String id;
    private String name;
    private String price;

    public ProductModel() {
        super();
    }

    public ProductModel(String id, String name, String price) {
        this.id = id;
        this.name = name;
        this.price = price;
    }

    @Override
    public int compareTo(ProductModel o) {
        if (this == o) {
            return 0;
        }
        int tmp = this.id.compareTo(o.getId());
        if (tmp != 0)
            return tmp;
        tmp = this.name.compareTo(o.getName());
        if (tmp != 0)
            return tmp;
        tmp = this.price.compareTo(o.getPrice());
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(name);
        out.writeUTF(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.name = in.readUTF();
        this.price = in.readUTF();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProductModel)) return false;

        ProductModel that = (ProductModel) o;

        if (getId() != null ? !getId().equals(that.getId()) : that.getId() != null) return false;
        if (getName() != null ? !getName().equals(that.getName()) : that.getName() != null) return false;
        return getPrice() != null ? getPrice().equals(that.getPrice()) : that.getPrice() == null;

    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = 31 * result + (getName() != null ? getName().hashCode() : 0);
        result = 31 * result + (getPrice() != null ? getPrice().hashCode() : 0);
        return result;
    }
}
