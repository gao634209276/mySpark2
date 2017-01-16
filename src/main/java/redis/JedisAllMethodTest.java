package redis;


import org.junit.AfterClass;
import org.junit.BeforeClass;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

public class JedisAllMethodTest {
	static Jedis jedis = null;
	static HostAndPort hnp = new HostAndPort("10.0.50.11", Protocol.DEFAULT_PORT);

	/**
	 * 初始化连接
	 *
	 * @return void
	 * @throws Exception
	 * @date 2015-12-16 上午8:54:01
	 */
	@BeforeClass  // junit 提供的@BeforeClass标签，    beforeClass 标签只执行一次
	public static void setUp() throws Exception {
		//连接jedis 的第一种方式
		jedis = new Jedis(hnp.getHost(), hnp.getPort(), 500);
		jedis.connect();
		//jedis.auth("foobared");// redis auth 命令 验证 redis 密码，需要在redis.conf配置文件中，开启  requirepass foobared 参数，
		jedis.configSet("timeout", "300");// redis config 命令
		jedis.flushAll();// redis flushall 命令 清空所有key 及清空所有数据

		//连接jedis的第二种方式
		/* 使用 uri 连接数据库
 	    jedis = new Jedis(new URI("redis://:foobared@10.0.50.11:6379/4"), 15000,15000);// 4 表示连接的是 第5个数据库 url,超时时间,输入流超时时间
	    jedis.connect();
	    jedis.flushAll();// redis flushall 命令 清空所有key 及清空所有数据
	      */

		// 连接jedis的第三种方式
	    /*使用 jedisShardInfo 连接redis
		JedisShardInfo jedisShardInfo = new JedisShardInfo("redis://:foobared@10.0.50.11:6379/1");// /1 表示连接的是 第二个数据库 ；foobared 数据库的密码
		jedisShardInfo.setConnectionTimeout(150000);
		jedisShardInfo.setSoTimeout(150000);
		jedis = new Jedis(jedisShardInfo);
	    jedis.connect();
	    jedis.flushAll();// redis flushall 命令 清空所有key 及清空所有数据
	    */

	}

	/**
	 * 关闭连接
	 */
	@AfterClass
	public static void tearDown() { // 当标签为 AfterClass BeforeClass 时方法 必须为static  为 before  after 时不需要
		jedis.disconnect();// 最终调用的是  Connection 中的  disconnect  作用是关闭流，关闭socket
	}

	/**
	 *   redis 中  对 字符串 和key的操作的演示
	 */
	/*@Test
	public void jedisStringKeyTest() throws InterruptedException, ParseException{
		System.out.println("===测试jedis BEGIN==="); // sotimeOut设置socket调用InputStream读数据的超时时间
		jedis.set("test", "test");
		System.out.println(jedis.get("test"));
		jedis.append("test", "test2");// 最终是将 字符串转换为字节数组通过流的形式写入
		System.out.println(jedis.get("test"));

		*//*
		 * NX|XX, NX --有此参数时只能 set 不存在的key,如果给已经存在的key set 值则不生效， XX -- 此参数只能设置已经存在的key 的值，不存在的不生效
		 * EX|PX, key 的存在时间: EX = seconds; PX = milliseconds
		 * *//*
		// 验证  NX XX
		String status = jedis.set("test", "test234",SetParams.setParams().nx());// status 为null 则说明操作失败
		System.out.println("测试SetParams.setParams().nx()给已经存在的key set值  操作状态："+status + "操作值"+jedis.get("test")); // 输出值testtest2
		status = jedis.set("test1", "test2222",SetParams.setParams().nx());//status 为ok
		System.out.println("测试SetParams.setParams().nx()给不存在的key set值  操作状态 操作状态："+status + "操作值"+jedis.get("test1"));// 输出值为test2222

		status = jedis.set("test1", "test342222",SetParams.setParams().xx());// status 为ok
		System.out.println(" 测试SetParams.setParams().xx()给已经存在的key set值  操作状态 操作状态："+status + "操作值"+jedis.get("test1"));// 输出值为 test342222
		status = jedis.set("test21", "test34212222",SetParams.setParams().xx());// status 为null
		System.out.println("测试SetParams.setParams().xx()给不存在的key set值  操作状态 操作状态 操作状态："+status + "操作值"+jedis.get("test21"));// 输出值为null

		// 验证 EX|PX
		status = jedis.set("testEX", "testEX",SetParams.setParams().ex(1));
		System.out.println("测试 ex  操作状态："+status+"操作值:"+jedis.get("testEX"));// 输出 testEX
		Thread.sleep(1002);
		System.out.println("测试ex 1 秒后自动销毁，输出null 操作状态："+status+"操作值:"+jedis.get("testEX"));// 1 秒后自动销毁，输出null

		status = jedis.set("testPX", "testEX",SetParams.setParams().px(100));
		System.out.println("测试 px  操作状态："+status+"操作值:"+jedis.get("testPX"));// 输出 testEX
		Thread.sleep(101);
		System.out.println("测试px 1 秒后自动销毁 操作状态："+status+"操作值:"+jedis.get("testPX"));// 1 秒后自动销毁，输出null


		// Note: 因为SET 命令可以通过参数来实现和SETNX 、SETEX 和PSETEX 三个命令的效果，所以将来的Redis 版本可能会废弃并最终移除SETNX 、SETEX 和PSETEX 这三个命令
		//setNx 将key 的值设为value ，当且仅当key 不存在。若给定的key 已经存在，则SETNX 不做任何动作。 SETNX 是『SET if Not eXists』(如果不存在，则SET) 的简写
		// 类似于 set(key,value,nx)
		// 当key 不存在时
		long   n = jedis.setnx("key6", "value6");
		System.out.println(n!=0?"set 成功"+jedis.get("key6"):"set 失败"); // set 成功返回 1  set失败返回 0
		// 当key 存在时
		n = jedis.setnx("key6", "value6");
		System.out.println(n!=0?"set 成功"+jedis.get("key6")+n:"set 失败"+jedis.get("key6")+n);

		// setex 类似于 执行 set(key,value) 在执行   EXPIRE  类似于  set(key,value,ex)  设置生存时间是 秒级的
		status =  jedis.setex("key7", 1, "value7");
		System.out.println(status+jedis.get("key7"));
		Thread.sleep(1000);
		System.out.println(jedis.get("key7"));
		jedis.psetex("key20", 10, "value20");// 设置毫秒级的生存时间,类似于  set(key,value,px)

		// exists  检查给定key(多个 key值) 是否存在。 返回值： 若 所有key都不存在，返回0 ，若存在 则返回 存在key的数量
		System.out.println("===测试 exists 命令： ====");
		n = jedis.exists("test1 test test2".split(" ")); //
		int count = "test1 test test2".split(" ").length - (int) n;
		System.out.println( n != 0 ? "key 存在 个数为："+n+" 不存在个数为："+count:"key 不存在"+n);

		// exist 检查 单个 key 值是否存在   返回值： 如果key 值存在 返回 true ,如果不存在 在返回false

		boolean exists =  jedis.exists("test");// 返回true
		System.out.println(exists);

		// del 删除给定的一个或多个key 。不存在的key 会被忽略  返回值： 返回删除key 的数量，
		System.out.println("===测试 del 命令：=== ");
		n = jedis.del("test1 test test2".split(" "));
		System.out.println("删除key 的数量为"+n);
		System.out.println(jedis.get("test")+jedis.get("test1"));// 返回null  说明已经删除了

		jedis.set("testDel", "testDel");
		n = jedis.del("testDel");
		System.out.println("删除key 的数量为"+n);

		// type 命令
		System.out.println("===测试 type 命令：=== ");
		String type = jedis.type("test"); // test 不存在 返回none
		System.out.println("存储类型为："+type);
		jedis.set("testType", "testType");
		type = jedis.type("testType"); // test 返回String
		System.out.println("存储类型为："+type);

		jedis.flushAll();
		// mset 命令 批量 插入   插入所有的值，如果key值已经存在，则进行覆盖   插入成功 返回ok 总是插入成功
		System.out.println("===测试 mset 命令：=== ");
		status = jedis.mset("fruit apple drink beer food cookies".split(" "));
		System.out.println("OK".equals(status)?"批量插入成功":"批量插入失败");

		// msetnx 批量插入 不存在的key ,如果有已存在的key 则返回  0  插入失败
		System.out.println("===测试 msetnx 命令：=== ");
		n = jedis.msetnx("apple fruit beer drink cookies food fruit cookies".split(" "));// 返回 0 插入失败，插入的里面不能有 已经存在的key
		System.out.println(n!=0?"批量插入成功"+n:"批量插入失败 "+n);

		n = jedis.msetnx("apple fruit beer drink cookies food".split(" "));// 返回 1 插入成功
		System.out.println(n!=0?"批量插入成功"+n:"批量插入失败 "+n);

		// keys 命令  根据正则表达式 取得相匹配的  key
		System.out.println("===测试 keys 命令：==== ");
		Set<String> keySet = jedis.keys("*");
		Iterator<String> it = keySet.iterator();
		String s = "";
		while(it.hasNext()){
			s = s+ it.next()+" ";

		}
		System.out.println("正则所匹配的key:"+s);

		// randomKey 命令 从当前数据库中随机返回(不删除) 一个key 。
		System.out.println("====测试 randomKey 命令：===");
		int m = 0;
		while(m < 4){
			System.out.println(jedis.randomKey());
			m++;
		}
		System.out.println("====测试 rename 命令：===");
//		 status = jedis.rename("apple", "apple");// 重命名 key 时，如果 oldKey 与 newkey 一样则返回一个错误，执行不通过
		status = jedis.rename("apple", "appleRename");
		System.out.println(jedis.get("fruit"));// apple
		status = jedis.rename("appleRename", "fruit");// 如果 newKey 已经存在则覆盖

		System.out.println("OK".equals(status)?"更新key成功"+jedis.get("fruit"):"更新key失败");//fruit

		System.out.println("====测试 renamenx 命令：===");//
		n = jedis.renamenx("fruit", "apple");//新的key 只能是不存在的key
		System.out.println(n!= 0?"更新key成功":"更新失败");
//		 n = jedis.renamenx("fruit", "cookies");//新的key 存在 返回一个错误
//		 System.out.println(n!= 0?"更新key成功":"更新失败");

		//expire 设置生存时间
		System.out.println("====测试 expire 命令：===");
		n = jedis.expire("cookies", 1);// 设置生存时间 以秒 为单位
		System.out.println(jedis.get("cookies"));
		System.out.println("休眠1秒");
		Thread.sleep(1000);
		System.out.println(n!=0?"设置生存时间成功"+jedis.get("cookies"):"设置生存时间失败");
		// 针对 cookies 进行更名操作，查看是否 还具有生存时间
		jedis.flushAll();
		jedis.mset("key1 value1 key2 value2 key3 value3 key4 value4 key5 value5".split(" "));
		n = jedis.expire("key1", 2);
		jedis.rename("key1", "key6");
		System.out.println(jedis.get("key6"));
		Thread.sleep(2000);
		System.out.println(jedis.get("key6"));// 输出null  说明 rename 后 key6 继承了 key1的生存时间。
		jedis.del("key6");// 删除key6  del 会删除生存时间
		jedis.set("key6", "value1");
		System.out.println(jedis.get("key6"));
		Thread.sleep(2000);
		System.out.println(jedis.get("key6"));



		jedis.flushAll();
		jedis.mset("key1 value1 key2 value2 key3 value3 key4 value4 key5 value5".split(" "));

		// ttl   查看剩余生存时间   expireAt EXPIREAT 的作用和EXPIRE 类似，都用于为key 设置生存时间。不同在于EXPIREAT 命令接受的时间参数是UNIX 时间戳(unix timestamp)。
//		 jedis.expireAt("key1",20149765902363l);
//		 n = jedis.ttl("key1");// 返回 key1上的 剩余时间
//		 System.out.println(n);// 如果不存在 返回 -1

		//move
		System.out.println("测试 move命令");
		// key1 不存在 1 中时 将 0 中的 key1 移动到 1 中
		jedis.select(1);// 切换 DB 为 1
		System.out.println(jedis.exists("key1")?"key1 存在 库 1 中":"key1 不存在 库 1 中");// 查询 DB1 中是否 包含 key1
		jedis.select(0);//切换 到 0
		n = jedis.move("key1", 1);
		jedis.select(1);// 切换 DB 为 1
		System.out.println(n+jedis.get("key1"));// 查询 DB1 中是否 包含 key1

		// key2 不存在 1 库中时，将 1库中的key2 移动到 key1
		jedis.select(1);// 切换 DB 为 1
		System.out.println(jedis.exists("key2")?"key2 存在 库 1 中":"key2 不存在 库 1 中");// 查询 DB1 中是否 包含 key1
		n = jedis.move("key2", 0);
		System.out.println(n==0?"移动失败":"移动成功");// 查询 DB1 中是否 包含 key1


		// 当两个库中都存在 key1 时
		jedis.select(0);
		jedis.set("key1", "ivalue11");
		jedis.select(1);// 切换 DB 为 1
		System.out.println(jedis.exists("key1")?"key1 存在 库 1 中"+jedis.get("key1"):"key1 不存在 库 1 中");// 查询 DB1 中是否 包含 key1
		n = jedis.move("key1", 0);
		System.out.println("1 库中的 key1 value:"+jedis.get("key1"));
		jedis.select(0);// 切换 DB 为 1
		System.out.println(n==0?"移动失败 0库中的key1:"+jedis.get("key1"):"移动成功");// 查询 DB1 中是否 包含 key1

		//getset  返回 key 对应的旧值，set 新值  如果 key 在库中不存在
		jedis.flushAll();
		jedis.mset("key1 value1 key2 value2 key3 value3 key4 value4 key5 value5".split(" "));
		jedis.select(0);
		String key1= jedis.getSet("key1", "value11");// 返回旧值  set 新值
		System.out.println("key1 对应的旧值："+key1 + "key1 新值："+jedis.get("key1"));


		// mget 返回所有(一个或多个) 给定key 的值。 如果给定的key 里面，有某个key 不存在或者 value 非String，，那么这个key 返回特殊值null  。因此，该命令永不失败
		jedis.flushAll();
		jedis.mset("key1 value1 key2 value2 key3 value3 key4 value4 key5 value5".split(" "));
		List<String> list = jedis.mget("key1 key2 key3 key4 key5 key6".split(" "));
		for(String value : list){
			System.out.print(value+" ");
		}
		System.out.println();

		//decr
//		 n = jedis.decr("key6"); // 由于value 不是数字 所有返回错误
		// key 存在的情况
		jedis.set("key8", "10");
		n = jedis.decr("key8");// 返回值： 执行DECR 命令之后key 的值。 所以 n = 9
		System.out.println("返回值： 执行DECR 命令之后key 的值。:"+ n+"  jedis.get :"+jedis.get("key8")+"  key8 的 type ："+jedis.type("key8"));

		//key 不存在的情况  先将 key 初始化为 0 在减去1
		n = jedis.decr("key9");// 返回值： 执行DECR 命令之后key 的值。 所以 n = 9
		System.out.println("返回值： 执行DECR 命令之后key 的值。:"+ n+"  jedis.get :"+jedis.get("key9")+"  key9 的 type： "+jedis.type("key9"));


		//decrby
		jedis.set("key8", "10");
		n = jedis.decrBy("key8",5);// 返回值： 执行decrby  在原原有的基础上减去 5  n = 5
		System.out.println("返回值： 执行decrby命令之后key 的值。:"+ n+"jedis.get :"+jedis.get("key8")+"key8 的 type ："+jedis.type("key8"));

		//key 不存在的情况  先将 key 初始化为 0 在减去7
		n = jedis.decrBy("key10",7);// 返回值： 执行DECRBy 命令之后key 的值。 所以 n = -7
		System.out.println("返回值： 执行decrby命令之后key 的值。:"+ n+"  jedis.get :"+jedis.get("key10")+"  key10 的 type： "+jedis.type("key10"));

		// incr
		jedis.set("key8", "10");
		n = jedis.incr("key8");// 返回值： 执行DECR 命令之后key 的值。 所以 n = 11
		System.out.println("返回值： 执行incr 命令之后key 的值。:"+ n+"  jedis.get :"+jedis.get("key8")+"  key8 的 type ："+jedis.type("key8"));

		//key 不存在的情况  先将 key 初始化为 0 在加1
		n = jedis.incr("key12");//返回值： 执行incr 命令之后key 的值。 所以 n = 1
		System.out.println("返回值： 执行incr 命令之后key 的值。:"+ n+"  jedis.get :"+jedis.get("key12")+"  key12 的 type： "+jedis.type("key12"));

		//incrby
		jedis.set("key8", "10");
		n = jedis.incrBy("key8",5);// 返回值： 执行decrby  在原原有的基础上加上 5  n = 15
		System.out.println("返回值： 执行decrby命令之后key 的值。:"+ n+"jedis.get :"+jedis.get("key8")+"key8 的 type ："+jedis.type("key8"));

		//key 不存在的情况  先将 key 初始化为 0 在加7  int 类型
		n = jedis.incrBy("key13",7);// 返回值： 执行incrBy命令之后key 的值。 所以 n = 7
		System.out.println("返回值： 执行incrBy命令之后key 的值。:"+ n+"  jedis.get :"+jedis.get("key13")+"  key13 的 type： "+jedis.type("key13"));

		// incrByFloat  输入的是 double 返回的 是double  上面的 incr  incrBy decr decrBy 都只能针对int 操作
		double incrDouble = jedis.incrByFloat("key15",66666.3333);// 返回值： 执行incrBy命令之后key 的值。 所以 n = 7
		System.out.println("返回值： 执行incrBy命令之后key 的值。:"+ incrDouble+"  jedis.get :"+jedis.get("key15")+"  key13 的 type： "+jedis.type("key15"));

		//append
		//key 存在的情况
		n = jedis.append("key4", "Append");//返回值 是 追加字符串后的 key4 value 的changd :12 key4 的length: 12 key4 的value: value4Append
		System.out.println("返回值 是 追加字符串后的 key4 value 的changd :"+ n +" key4 的length: "+jedis.get("key4").length() +" key4 的value: "+jedis.get("key4"));
		// key 不存在的情况  单纯的 set  新值
		n = jedis.append("key16", "key16Append");//返回值 是 追加字符串后的 key16 value 的changd :11 key16 的length: 11 key16 的value: key16Append
		System.out.println("返回值 是 追加字符串后的 key16 value 的changd :"+ n +" key16 的length: "+jedis.get("key16").length() +" key16 的value: "+jedis.get("key16"));

		// substr getrange 命令 目前没发现二者的区别，待以后研究
		String subStr = jedis.substr("key4",0,3);
		System.out.println("key4 返回的子字符串为： "+subStr );
		subStr = jedis.substr("key4",-2,-1);
		System.out.println("key4 返回的子字符串为： "+subStr );
		subStr = jedis.getrange("key4",0,3);//在<= 2.0 的版本里，GETRANGE 被叫作SUBSTR
		System.out.println("key4 返回的子字符串为： "+subStr );
		subStr = jedis.getrange("key4",-2,-1);//在<= 2.0 的版本里，GETRANGE 被叫作SUBSTR
		System.out.println("key4 返回的子字符串为： "+subStr );
		// setrange
		//设置的偏移量 4 小于原字符串的长度
		n = jedis.setrange("key4", 4, "333");// 从第五位开始 替换 将原有的字符串 替换为333   返回值 n 是字符串的长度 12
		System.out.println("key4 的value : "+jedis.get("key4")+"  返回字符串的长度： "+n); // key4 的value : valu333ppend  返回字符串的长度： 12
		// 设置的偏移量大于 原字符串的长度
		n = jedis.setrange("key4", 18, "222333");// 超出偏移量的地方用空格代替
		System.out.println("key4 的value : "+jedis.get("key4")+"  返回字符串的长度： "+n); //  key4 的value : valu333ppend     222333  返回字符串的长度： 24
		// 当key 为空时
		n = jedis.setrange("key22", 18, "222333");//  key为空时，相当于set 新值，但是set为位置是从17位开始，前面的全部用个空格代替
		System.out.println("key22 的value : "+jedis.get("key22")+"  返回字符串的长度： "+n); //  key22 的value :                222333  返回字符串的长度： 24

		//strlen
		n = jedis.strlen("key5");
		System.out.println("key5 的value： "+jedis.get("key5")+"返回 key5的长度： "+n);

		//bitcount  getbit  setbit bitop 这四个命令是针对字符串的二进制进行操作

		//• SCAN 命令用于迭代当前数据库中的数据库键。  hscan sscan zscan 在 集合 操作 list 操作中演示
		ScanResult<String> scanResult =  jedis.scan("0"); // 最简单的迭代，迭代数据库中的所有key 初始游标为 0 ，返回结果是封装好了的 ScanResult对象 ， 包含游标和 key 结果集
		scanResult.getCursor();// 返回的游标  继续用此 游标进行遍历，直到 返回 0 说明遍历完成。
		scanResult.getResult();// 返回的结果集 里面包含返回的所有的key

		// scan(curso,params) cursor 表示开始遍历的游标   params 是ScanParams 对象，此对象可以设置 每次返回的数量，以及遍历时的正则表达式
		ScanParams scanParams = new ScanParams();
		scanParams.count(100);// 表示一次返回的数量是100个 及 ScanResult.getResult 返回的list size 为 99
		scanParams.match("*");// 表示遍历所有的值  也可以 如 ： *11* 搜索key 中包含11 的key 注意：需要注意的是，对元素的模式匹配工作是在命令从数据集中取出元素之后，向客户端返回元素之前的这段时间内进行的，所以如果被迭代的数据集中只有少量元素和模式相匹配，那么迭代命令或许会在多次执行中都不返回任何元素。
		jedis.scan("0",scanParams);//
	}*/
}
