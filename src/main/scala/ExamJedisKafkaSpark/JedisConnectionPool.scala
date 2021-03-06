package ExamJedisKafkaSpark
  /**
    * Created by zx on 2017/10/20.
    */
  import java.{lang, util}

  import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

  object JedisConnectionPool{

    val config = new JedisPoolConfig()
    //最大连接数
    config.setMaxTotal(20)
    //最大空闲连接数
    config.setMaxIdle(10)
    //当调用borrow Object方法时，是否进行有效性检查 -->
    //检测链接是否活跃
    config.setTestOnBorrow(true)
    //10000代表超时时间（10秒） redes地址(slave2装着单机reredies)  6379端口  最大超时时间10s  密码123
    val pool = new JedisPool(config, "192.168.9.13", 6379, 10000, "123")
   //从链接池获得链接
    def getConnection(): Jedis = {
      pool.getResource
    }

    def main(args: Array[String]) {

      val conn = JedisConnectionPool.getConnection()
     /* conn.set("TOTAL_INCOME", "1000")
      val r1 = conn.get("TOTAL_INCOME")
     conn.incrBy("income", -50)
      val r2 = conn.get("income")
      println(r2)*/

      //获取所有的key,util.Set[String]这个是java集合
       val r: util.Set[String] = conn.keys("*")
      //把java集合转换成scala集合，下面这种for循环就能使用了不倒这个操作不了java集合
     import scala.collection.JavaConversions._
      for (p <- r) {
        println(p + " : " + conn.get(p))
      }

      conn.close()
    }
  }

