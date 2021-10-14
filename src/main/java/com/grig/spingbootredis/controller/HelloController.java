package com.grig.spingbootredis.controller;

import com.grig.spingbootredis.service.RedisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Map;

@RestController
public class HelloController {

    @Autowired
    private RedisService redisService;


    @GetMapping("/hello")
    public  String hello(){
        return "hello springboot";
    }

    @GetMapping("/redis")
    @ResponseBody
    public  Object redis(@RequestParam(value="methodname",required = true) String methodname, HttpServletRequest request){
        Object result = "";
        switch (methodname) {
            case "set1":
                result= redisService.set1("language","java");
                break;
            case "set2":
                result= redisService.set2("hobbys","旅游",60);
                break;
            case "del":
                result= redisService.del("hobbys");
            break;
            case "get1":
                result= redisService.get1("language");
            break;
            case "expire":
                result= redisService.expire("language",60);
            break;
            case "set3":
                String key="ProtocolA:Type";
                Map<String, String> value=new HashMap<>();
                value.put("01", "SmokeSensor");
                value.put("02", "TemperatureSensor");
                result= redisService.set3(key,value);
            break;
            case "get2":
                result= redisService.get2("ProtocolA:Type");
            break;
            case "rwSplittingWrite":
                result= redisService.rwSplittingWrite("读写分离测试","写入报错");
                break;
            case "rwSplittingRead":
                result= redisService.rwSplittingRead("language");
                break;
//            case "shardedJedisDemoWrite":
//                result= redisService.shardedJedisDemoWrite();
//                break;
//            case "shardedJedisDemoRead":
//                result= redisService.shardedJedisDemoRead();
//                break;
            case "shardedSentinelJedisDemoReadAll":
                result= redisService.shardedSentinelJedisDemoReadAll();
                break;
            case "shardedSentinelJedisDemoRead":
                result= redisService.shardedSentinelJedisDemoRead(request.getParameter("key"));
                break;
            case "shardedSentinelJedisDemoDel":
                result= redisService.shardedSentinelJedisDemoDel(request.getParameter("key"));
                break;
            case "shardedSentinelJedisDemoWrite":
                result= redisService.shardedSentinelJedisDemoWrite(request.getParameter("key"),request.getParameter("value"));
                break;
            default:
                break;
        }
        return  result;
    }
}
