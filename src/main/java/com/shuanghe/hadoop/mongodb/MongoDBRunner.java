package com.shuanghe.hadoop.mongodb;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * 操作Mongodb的主类
 * Created by yushuanghe on 2017/02/14.
 */
public class MongoDBRunner {
    /**
     * mongodb DBObject转换到hadoop的一个bean对象
     */
    static class PersonMongoDBWritable implements MongoDBWritable {

        private String name;
        private Integer age;
        private String sex = "";
        private int count = 1;

        @Override
        public void readFields(DBObject dbObject) {
            this.name = dbObject.get("name").toString();
            if (dbObject.get("age") != null)
                this.age = Double.valueOf(dbObject.get("age").toString()).intValue();
            else
                this.age = null;
//            this.sex = dbObject.get("sex").toString();
        }

        @Override
        public void write(DBCollection dbCollection) {
            DBObject dbObject = BasicDBObjectBuilder.start()
                    .add("age", this.age).add("count", this.count).get();
            dbCollection.insert(dbObject);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.name);
            if (this.age == null)
                out.writeBoolean(false);
            else {
                out.writeBoolean(true);
                out.writeInt(this.age);
            }
            out.writeUTF(this.sex);
            out.writeInt(this.count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.name = in.readUTF();
            if (in.readBoolean()) {
                this.age = in.readInt();
            } else {
                this.age = null;
            }
            this.sex = in.readUTF();
            this.count = in.readInt();
        }
    }

    static class MongoDBMapper extends Mapper<LongWritable, PersonMongoDBWritable, IntWritable, PersonMongoDBWritable> {
        @Override
        protected void map(LongWritable key, PersonMongoDBWritable value, Context context) throws IOException, InterruptedException {
            if (value.age == null) {
                System.out.print(value.name + "被过滤");
                return;
            }
            context.write(new IntWritable(value.age), value);
        }
    }

    static class MongoDBReducer extends Reducer<IntWritable, PersonMongoDBWritable, NullWritable, PersonMongoDBWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<PersonMongoDBWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (PersonMongoDBWritable value : values) {
                sum += value.count;
            }
            PersonMongoDBWritable personMongoDBWritable = new PersonMongoDBWritable();
            personMongoDBWritable.age = key.get();
            personMongoDBWritable.count = sum;
            context.write(NullWritable.get(), personMongoDBWritable);
        }
    }

    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("hadoop").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        Configuration conf = new Configuration();
                        // 设置intputformat的value 类
                        conf.setClass("mapreduce.mongo.split.value.class",
                                PersonMongoDBWritable.class, MongoDBWritable.class);
                        try {
                            Job job = Job.getInstance(conf, "自定义input/output format");
                            job.setJarByClass(MongoDBRunner.class);
                            job.setMapperClass(MongoDBMapper.class);
                            job.setReducerClass(MongoDBReducer.class);
                            job.setMapOutputKeyClass(IntWritable.class);
                            job.setMapOutputValueClass(PersonMongoDBWritable.class);
                            job.setOutputKeyClass(NullWritable.class);
                            job.setOutputValueClass(PersonMongoDBWritable.class);

                            job.setInputFormatClass(MongoDBInputFormat.class);
                            job.setOutputFormatClass(MongoDBOutputFormat.class);

                            job.waitForCompletion(true);
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );
    }
}
