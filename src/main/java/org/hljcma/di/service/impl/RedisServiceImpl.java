package org.hljcma.di.service.impl;

import org.hljcma.di.service.DiRedisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;

/**
 * 
 * @author
 * @desc resdis servicezzw
 *
 */
@Service
public class RedisServiceImpl implements DiRedisService {

	@Autowired
	private RedisTemplate<String, String> redisTemplate;

    @Override
    public void flushdb(){
        redisTemplate.execute(new RedisCallback<Object>() {
            public String doInRedis(RedisConnection connection) throws DataAccessException {
                connection.flushDb();
                return "ok";
            }
        });
    }

	@Override
	public boolean set(final String key, final String value) {
        ValueOperations<String,String> stringOperations = redisTemplate.opsForValue();
        stringOperations.set(key,value);
        return(true);
	}

    @Override
	public String get(final String key){
        ValueOperations<String,String> stringOperations = redisTemplate.opsForValue();
		return stringOperations.get(key);
	}

    @Override
    public boolean setlist(final String key, final String value) {
        ListOperations<String,String> liststring=redisTemplate.opsForList();
        liststring.rightPush(key,value);
        return(true);
    }

    @Override
    public String getlist(String key) {
        ListOperations<String,String> liststring=redisTemplate.opsForList();
        return(liststring.range(key,0,-1).toString());
    }
}

